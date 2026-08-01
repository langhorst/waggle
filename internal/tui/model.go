package tui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

type view int

const (
	viewChannels view = iota
	viewMessages
	viewDetail
)

// Detail tabs.
const (
	tabRaw = iota
	tabTree
	tabDiff
	tabDestinations
	tabCount
)

type (
	eventMsg struct{ ev events.Event }
	tickMsg  struct{}
)

// Model is the root Bubbletea model.
type Model struct {
	backend Backend
	ctx     context.Context

	eventCh      <-chan events.Event
	cancelEvents func()

	width, height int
	view          view
	errText       string

	// Channel list.
	channels   []engine.Info
	counts     map[string]map[message.State]int
	depths     map[string]map[string]int
	chanCursor int

	// Message list.
	selChannel string
	messages   []store.MessageSummary
	msgCursor  int
	dlqOnly    bool

	// Detail.
	detail    *store.MessageDetail
	detailTab int
	stages    []string // cyclable stages for the tree tab
	stageIdx  int
	tree      *message.Node
	treeDT    string
	treeErr   string
	diff      []message.DiffEntry
	diffErr   string
	scroll    int
}

// New builds the root model around a backend.
func New(backend Backend) Model {
	m := Model{
		backend: backend,
		ctx:     context.Background(),
		counts:  map[string]map[message.State]int{},
		depths:  map[string]map[string]int{},
		width:   100,
		height:  30,
	}
	m.eventCh, m.cancelEvents = backend.Subscribe(256)
	m.reloadChannels()
	return m
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(waitEvent(m.eventCh), tick())
}

func waitEvent(ch <-chan events.Event) tea.Cmd {
	return func() tea.Msg {
		ev, ok := <-ch
		if !ok {
			return nil
		}
		return eventMsg{ev}
	}
}

func tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case eventMsg:
		m.applyEvent(msg.ev)
		return m, waitEvent(m.eventCh)

	case tickMsg:
		m.refresh()
		return m, tick()
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		if m.cancelEvents != nil {
			m.cancelEvents()
		}
		return *m, tea.Quit
	}
	switch m.view {
	case viewChannels:
		return m.handleChannelsKey(msg)
	case viewMessages:
		return m.handleMessagesKey(msg)
	case viewDetail:
		return m.handleDetailKey(msg)
	}
	return *m, nil
}

func (m *Model) handleChannelsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		if m.chanCursor > 0 {
			m.chanCursor--
		}
	case "down", "j":
		if m.chanCursor < len(m.channels)-1 {
			m.chanCursor++
		}
	case "enter":
		if len(m.channels) > 0 {
			m.selChannel = m.channels[m.chanCursor].ID
			m.dlqOnly = false
			m.msgCursor = 0
			m.reloadMessages()
			m.view = viewMessages
		}
	case "r":
		m.reloadChannels()
	}
	return *m, nil
}

func (m *Model) handleMessagesKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.view = viewChannels
		m.reloadChannels()
	case "up", "k":
		if m.msgCursor > 0 {
			m.msgCursor--
		}
	case "down", "j":
		if m.msgCursor < len(m.messages)-1 {
			m.msgCursor++
		}
	case "d":
		m.dlqOnly = !m.dlqOnly
		m.msgCursor = 0
		m.reloadMessages()
	case "r":
		m.reloadMessages()
	case "enter":
		if len(m.messages) > 0 {
			m.openDetail(m.messages[m.msgCursor].ID)
		}
	}
	return *m, nil
}

func (m *Model) handleDetailKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.view = viewMessages
		m.scroll = 0
	case "tab", "right", "l":
		m.detailTab = (m.detailTab + 1) % tabCount
		m.scroll = 0
	case "shift+tab", "left", "h":
		m.detailTab = (m.detailTab + tabCount - 1) % tabCount
		m.scroll = 0
	case "up", "k":
		if m.scroll > 0 {
			m.scroll--
		}
	case "down", "j":
		m.scroll++
	case "s":
		if m.detailTab == tabTree && len(m.stages) > 1 {
			m.stageIdx = (m.stageIdx + 1) % len(m.stages)
			m.scroll = 0
			m.reloadTree()
		}
	case "r":
		if m.detail != nil {
			m.openDetail(m.detail.ID)
		}
	}
	return *m, nil
}

func (m *Model) applyEvent(ev events.Event) {
	switch ev.Type {
	case events.TypeResync:
		m.refresh()
	case events.TypeChannelStatus:
		m.reloadChannels()
	case events.TypeMessage:
		switch m.view {
		case viewChannels:
			m.reloadCounts(ev.ChannelID)
		case viewMessages:
			if ev.ChannelID == m.selChannel {
				m.reloadMessages()
			}
		case viewDetail:
			if m.detail != nil && ev.MessageID == m.detail.ID {
				m.openDetail(m.detail.ID)
			}
		}
	}
}

func (m *Model) refresh() {
	switch m.view {
	case viewChannels:
		m.reloadChannels()
	case viewMessages:
		m.reloadMessages()
	case viewDetail:
		if m.detail != nil {
			m.openDetail(m.detail.ID)
		}
	}
}

func (m *Model) reloadChannels() {
	m.channels = m.backend.Channels()
	if m.chanCursor >= len(m.channels) {
		m.chanCursor = max(0, len(m.channels)-1)
	}
	for _, ch := range m.channels {
		m.reloadCounts(ch.ID)
	}
}

func (m *Model) reloadCounts(channelID string) {
	if counts, err := m.backend.MessageCounts(m.ctx, channelID); err == nil {
		m.counts[channelID] = counts
	}
	if depth, err := m.backend.QueueDepth(m.ctx, channelID); err == nil {
		m.depths[channelID] = depth
	}
}

func (m *Model) reloadMessages() {
	q := store.ListQuery{Limit: 200}
	if m.dlqOnly {
		q.State = message.StateError
	}
	list, err := m.backend.ListMessages(m.ctx, m.selChannel, q)
	if err != nil {
		m.errText = err.Error()
		return
	}
	m.errText = ""
	m.messages = list
	if m.msgCursor >= len(m.messages) {
		m.msgCursor = max(0, len(m.messages)-1)
	}
}

func (m *Model) openDetail(id int64) {
	detail, err := m.backend.GetMessage(m.ctx, id)
	if err != nil {
		m.errText = err.Error()
		return
	}
	m.errText = ""
	m.detail = detail
	m.view = viewDetail

	// Stages for the tree tab: received, transformed (when present), and
	// each destination with a stored payload.
	m.stages = []string{engine.StageReceived}
	if len(detail.Transformed) > 0 {
		m.stages = append(m.stages, engine.StageTransformed)
	}
	for _, d := range detail.Destinations {
		m.stages = append(m.stages, "dest:"+d.DestinationID)
	}
	if m.stageIdx >= len(m.stages) {
		m.stageIdx = 0
	}
	m.reloadTree()
	m.reloadDiff()
}

func (m *Model) reloadTree() {
	tree, dt, err := m.backend.MessageTree(m.ctx, m.detail.ID, m.stages[m.stageIdx])
	if err != nil {
		m.tree, m.treeErr = nil, err.Error()
		return
	}
	m.tree, m.treeDT, m.treeErr = tree, dt, ""
}

func (m *Model) reloadDiff() {
	diff, err := m.backend.MessageDiff(m.ctx, m.detail.ID, "")
	if err != nil {
		m.diff, m.diffErr = nil, err.Error()
		return
	}
	m.diff, m.diffErr = diff, ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
