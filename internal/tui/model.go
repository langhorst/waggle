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

// Messages delivered to Update. Backend I/O never happens inside Update:
// every load runs as a tea.Cmd and comes back as one of the loaded
// messages below, so a slow daemon stalls a command, not the screen.
type (
	eventMsg struct{ ev events.Event }
	tickMsg  struct{}

	channelsMsg struct {
		channels []engine.ChannelSummary
		err      error
	}
	channelMsg struct {
		summary engine.ChannelSummary
		err     error
	}
	messagesMsg struct {
		channelID string
		list      []store.MessageSummary
		err       error
	}
	detailMsg struct {
		detail *store.MessageDetail
		err    error
	}
	treeMsg struct {
		id    int64
		stage string
		tree  *message.Node
		dt    string
		err   error
	}
	diffMsg struct {
		id   int64
		diff []message.DiffEntry
		err  error
	}
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
	channels   []engine.ChannelSummary
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

// New builds the root model around a backend. Nothing is loaded until
// Init runs.
func New(backend Backend) Model {
	m := Model{
		backend: backend,
		ctx:     context.Background(),
		width:   100,
		height:  30,
	}
	m.eventCh, m.cancelEvents = backend.Subscribe(256)
	return m
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.loadChannels(), waitEvent(m.eventCh), tick())
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

// Update reacts to msg and re-arms the event and tick subscriptions.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	next, cmd := m.update(msg)
	switch msg.(type) {
	case eventMsg:
		cmd = tea.Batch(cmd, waitEvent(next.eventCh))
	case tickMsg:
		cmd = tea.Batch(cmd, tick())
	}
	return next, cmd
}

// update is Update without the subscription plumbing: it returns only the
// load commands a message provokes, which is what tests drive.
func (m Model) update(msg tea.Msg) (Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	case eventMsg:
		return m, m.applyEvent(msg.ev)
	case tickMsg:
		return m, m.refresh()

	case channelsMsg:
		if msg.err != nil {
			m.errText = msg.err.Error()
			return m, nil
		}
		m.errText = ""
		m.channels = msg.channels
		if m.chanCursor >= len(m.channels) {
			m.chanCursor = max(0, len(m.channels)-1)
		}
		return m, nil
	case channelMsg:
		if msg.err != nil {
			return m, nil
		}
		for i := range m.channels {
			if m.channels[i].ID == msg.summary.ID {
				m.channels[i] = msg.summary
			}
		}
		return m, nil
	case messagesMsg:
		if msg.channelID != m.selChannel {
			return m, nil // stale: the operator moved on
		}
		if msg.err != nil {
			m.errText = msg.err.Error()
			return m, nil
		}
		m.errText = ""
		m.messages = msg.list
		if m.msgCursor >= len(m.messages) {
			m.msgCursor = max(0, len(m.messages)-1)
		}
		return m, nil
	case detailMsg:
		if msg.err != nil {
			m.errText = msg.err.Error()
			return m, nil
		}
		m.errText = ""
		m.detail = msg.detail
		m.view = viewDetail
		// Stages for the tree tab, the same list the web UI offers.
		m.stages = engine.Stages(msg.detail)
		if m.stageIdx >= len(m.stages) {
			m.stageIdx = 0
		}
		return m, tea.Batch(m.loadTree(), m.loadDiff())
	case treeMsg:
		if m.detail == nil || msg.id != m.detail.ID || msg.stage != m.stages[m.stageIdx] {
			return m, nil // stale
		}
		if msg.err != nil {
			m.tree, m.treeErr = nil, msg.err.Error()
			return m, nil
		}
		m.tree, m.treeDT, m.treeErr = msg.tree, msg.dt, ""
		return m, nil
	case diffMsg:
		if m.detail == nil || msg.id != m.detail.ID {
			return m, nil
		}
		if msg.err != nil {
			m.diff, m.diffErr = nil, msg.err.Error()
			return m, nil
		}
		m.diff, m.diffErr = msg.diff, ""
		return m, nil
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		if m.cancelEvents != nil {
			m.cancelEvents()
		}
		return m, tea.Quit
	}
	switch m.view {
	case viewChannels:
		return m.handleChannelsKey(msg)
	case viewMessages:
		return m.handleMessagesKey(msg)
	case viewDetail:
		return m.handleDetailKey(msg)
	}
	return m, nil
}

func (m Model) handleChannelsKey(msg tea.KeyMsg) (Model, tea.Cmd) {
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
			m.messages = nil
			m.view = viewMessages
			return m, m.loadMessages()
		}
	case "r":
		return m, m.loadChannels()
	}
	return m, nil
}

func (m Model) handleMessagesKey(msg tea.KeyMsg) (Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.view = viewChannels
		return m, m.loadChannels()
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
		return m, m.loadMessages()
	case "r":
		return m, m.loadMessages()
	case "enter":
		if len(m.messages) > 0 {
			return m, m.loadDetail(m.messages[m.msgCursor].ID)
		}
	}
	return m, nil
}

func (m Model) handleDetailKey(msg tea.KeyMsg) (Model, tea.Cmd) {
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
			return m, m.loadTree()
		}
	case "r":
		if m.detail != nil {
			return m, m.loadDetail(m.detail.ID)
		}
	}
	return m, nil
}

// applyEvent turns a bus event into the reload it warrants for the current
// view. Events carry identifiers only; the reload fetches the details.
func (m Model) applyEvent(ev events.Event) tea.Cmd {
	switch ev.Type {
	case events.TypeResync:
		return m.refresh()
	case events.TypeChannelStatus:
		return m.loadChannels()
	case events.TypeMessage:
		switch m.view {
		case viewChannels:
			return m.loadChannel(ev.ChannelID)
		case viewMessages:
			if ev.ChannelID == m.selChannel {
				return m.loadMessages()
			}
		case viewDetail:
			if m.detail != nil && ev.MessageID == m.detail.ID {
				return m.loadDetail(m.detail.ID)
			}
		}
	}
	return nil
}

// refresh reloads whatever the current view shows.
func (m Model) refresh() tea.Cmd {
	switch m.view {
	case viewChannels:
		return m.loadChannels()
	case viewMessages:
		return m.loadMessages()
	case viewDetail:
		if m.detail != nil {
			return m.loadDetail(m.detail.ID)
		}
	}
	return nil
}

// ---- load commands: the only place the backend is called ----

func (m Model) loadChannels() tea.Cmd {
	backend, ctx := m.backend, m.ctx
	return func() tea.Msg {
		channels, err := backend.ChannelSummaries(ctx)
		return channelsMsg{channels: channels, err: err}
	}
}

func (m Model) loadChannel(id string) tea.Cmd {
	backend, ctx := m.backend, m.ctx
	return func() tea.Msg {
		summary, err := backend.ChannelSummary(ctx, id)
		return channelMsg{summary: summary, err: err}
	}
}

func (m Model) loadMessages() tea.Cmd {
	backend, ctx, id := m.backend, m.ctx, m.selChannel
	q := store.ListQuery{Limit: 200}
	if m.dlqOnly {
		q.State = message.StateError
	}
	return func() tea.Msg {
		list, err := backend.ListMessages(ctx, id, q)
		return messagesMsg{channelID: id, list: list, err: err}
	}
}

func (m Model) loadDetail(id int64) tea.Cmd {
	backend, ctx := m.backend, m.ctx
	return func() tea.Msg {
		detail, err := backend.GetMessage(ctx, id)
		return detailMsg{detail: detail, err: err}
	}
}

func (m Model) loadTree() tea.Cmd {
	if m.detail == nil || len(m.stages) == 0 {
		return nil
	}
	backend, ctx, id, stage := m.backend, m.ctx, m.detail.ID, m.stages[m.stageIdx]
	return func() tea.Msg {
		tree, dt, err := backend.MessageTree(ctx, id, stage)
		return treeMsg{id: id, stage: stage, tree: tree, dt: dt, err: err}
	}
}

func (m Model) loadDiff() tea.Cmd {
	if m.detail == nil {
		return nil
	}
	backend, ctx, id := m.backend, m.ctx, m.detail.ID
	return func() tea.Msg {
		diff, err := backend.MessageDiff(ctx, id, "")
		return diffMsg{id: id, diff: diff, err: err}
	}
}
