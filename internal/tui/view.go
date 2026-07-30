package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/langhorst/integration-channel/internal/channel"
	"github.com/langhorst/integration-channel/internal/message"
)

var (
	titleStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("55")).Padding(0, 1)
	headerStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("245"))
	cursorStyle   = lipgloss.NewStyle().Background(lipgloss.Color("236")).Bold(true)
	helpStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	errStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
	tabStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Padding(0, 1)
	tabActiveStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("55")).Padding(0, 1)

	stateColors = map[message.State]string{
		message.StateReceived:    "6",
		message.StateFiltered:    "8",
		message.StateTransformed: "4",
		message.StateQueued:      "3",
		message.StateSent:        "2",
		message.StateError:       "1",
	}
	statusColors = map[channel.Status]string{
		channel.StatusStarted: "2",
		channel.StatusPaused:  "3",
		channel.StatusStopped: "8",
	}
)

func stateBadge(s message.State) string {
	color, ok := stateColors[s]
	if !ok {
		color = "7"
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(color)).Bold(true).Render(string(s))
}

func (m Model) View() string {
	var body string
	switch m.view {
	case viewChannels:
		body = m.viewChannels()
	case viewMessages:
		body = m.viewMessages()
	case viewDetail:
		body = m.viewDetail()
	}
	out := titleStyle.Render("integration-channel — observer") + "\n\n" + body
	if m.errText != "" {
		out += "\n" + errStyle.Render("error: "+m.errText)
	}
	return out
}

func (m Model) viewChannels() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf("%-20s %-24s %-9s %8s %8s %8s %8s %7s", "CHANNEL", "NAME", "STATUS", "RECV", "SENT", "ERROR", "FILT", "QUEUE")))
	b.WriteByte('\n')
	if len(m.channels) == 0 {
		b.WriteString(helpStyle.Render("no channels configured"))
		b.WriteByte('\n')
	}
	for i, ch := range m.channels {
		counts := m.counts[ch.ID]
		depth := 0
		for _, n := range m.depths[ch.ID] {
			depth += n
		}
		status := lipgloss.NewStyle().Foreground(lipgloss.Color(statusColors[ch.Status])).Render(string(ch.Status))
		line := fmt.Sprintf("%-20s %-24s %-9s %8d %8d %8d %8d %7d",
			truncate(ch.ID, 20), truncate(ch.Name, 24), status,
			counts[message.StateReceived]+counts[message.StateTransformed],
			counts[message.StateSent], counts[message.StateError],
			counts[message.StateFiltered], depth)
		if i == m.chanCursor {
			line = cursorStyle.Render("> " + line)
		} else {
			line = "  " + line
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("\n" + helpStyle.Render("↑/↓ select · enter messages · r refresh · q quit"))
	return b.String()
}

func (m Model) viewMessages() string {
	var b strings.Builder
	title := "channel " + m.selChannel
	if m.dlqOnly {
		title += "  [errors only]"
	}
	b.WriteString(headerStyle.Render(title) + "\n")
	b.WriteString(headerStyle.Render(fmt.Sprintf("%8s  %-12s %-12s  %s", "ID", "STATE", "RECEIVED", "INFO")))
	b.WriteByte('\n')
	if len(m.messages) == 0 {
		b.WriteString(helpStyle.Render("no messages") + "\n")
	}
	visible := m.messages
	maxRows := max(5, m.height-8)
	start := 0
	if m.msgCursor >= maxRows {
		start = m.msgCursor - maxRows + 1
	}
	if start+maxRows < len(visible) {
		visible = visible[start : start+maxRows]
	} else if start < len(visible) {
		visible = visible[start:]
	}
	for i, msg := range visible {
		info := msg.ErrorText
		if info == "" && msg.ReplayOf != 0 {
			info = fmt.Sprintf("replay of #%d", msg.ReplayOf)
		}
		line := fmt.Sprintf("%8d  %-22s %-12s  %s",
			msg.ID, stateBadge(msg.State), msg.ReceivedAt.Format("15:04:05.000"), truncate(info, 60))
		if start+i == m.msgCursor {
			line = cursorStyle.Render("> " + line)
		} else {
			line = "  " + line
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("\n" + helpStyle.Render("↑/↓ select · enter inspect · d errors-only · r refresh · esc channels · q quit"))
	return b.String()
}

func (m Model) viewDetail() string {
	if m.detail == nil {
		return helpStyle.Render("no message selected")
	}
	d := m.detail
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf("message #%d · channel %s · ", d.ID, d.ChannelID)))
	b.WriteString(stateBadge(d.State))
	if d.ReplayOf != 0 {
		b.WriteString(headerStyle.Render(fmt.Sprintf(" · replay of #%d", d.ReplayOf)))
	}
	b.WriteString("\n" + helpStyle.Render("correlation "+d.CorrelationID+" · received "+d.ReceivedAt.Format("2006-01-02 15:04:05.000")) + "\n\n")

	names := []string{"Raw", "Tree", "Diff", "Destinations"}
	var tabs []string
	for i, n := range names {
		if i == m.detailTab {
			tabs = append(tabs, tabActiveStyle.Render(n))
		} else {
			tabs = append(tabs, tabStyle.Render(n))
		}
	}
	b.WriteString(strings.Join(tabs, " ") + "\n\n")

	var content string
	switch m.detailTab {
	case tabRaw:
		content = m.renderRaw()
	case tabTree:
		content = m.renderTree()
	case tabDiff:
		content = m.renderDiff()
	case tabDestinations:
		content = m.renderDestinations()
	}
	b.WriteString(m.scrollWindow(content))
	b.WriteString("\n" + helpStyle.Render("tab/←/→ switch · ↑/↓ scroll · s cycle stage (tree) · esc back · q quit"))
	return b.String()
}

func (m Model) renderRaw() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf("raw inbound (%s, %d bytes)", m.detail.DataType, len(m.detail.Raw))) + "\n")
	b.WriteString(displayable(string(m.detail.Raw)) + "\n")
	if len(m.detail.Transformed) > 0 {
		dt := m.detail.TransformedDataType
		if dt == "" {
			dt = m.detail.DataType
		}
		b.WriteString("\n" + headerStyle.Render(fmt.Sprintf("transformed (%s, %d bytes)", dt, len(m.detail.Transformed))) + "\n")
		b.WriteString(displayable(string(m.detail.Transformed)) + "\n")
	}
	if m.detail.ErrorText != "" {
		b.WriteString("\n" + errStyle.Render(m.detail.ErrorText) + "\n")
	}
	return b.String()
}

func (m Model) renderTree() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render(fmt.Sprintf("stage: %s (%s) — press s to cycle", m.stages[m.stageIdx], m.treeDT)) + "\n")
	if m.treeErr != "" {
		b.WriteString(errStyle.Render(m.treeErr))
		return b.String()
	}
	renderNode(&b, m.tree, 0)
	return b.String()
}

func renderNode(b *strings.Builder, n *message.Node, depth int) {
	if n == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	if depth > 0 { // skip the synthetic root's own line
		if n.IsLeaf() {
			fmt.Fprintf(b, "%s%s: %s\n", indent, headerStyle.Render(n.Name), displayable(n.Value))
		} else {
			fmt.Fprintf(b, "%s%s\n", indent, headerStyle.Render(n.Name))
		}
	}
	for _, c := range n.Children {
		renderNode(b, c, depth+1)
	}
}

func (m Model) renderDiff() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render("received → transformed") + "\n")
	if m.diffErr != "" {
		b.WriteString(errStyle.Render(m.diffErr))
		return b.String()
	}
	if len(m.diff) == 0 {
		b.WriteString(helpStyle.Render("no differences"))
		return b.String()
	}
	for _, e := range m.diff {
		switch e.Op {
		case message.DiffChanged:
			fmt.Fprintf(&b, "%s %s: %s → %s\n",
				lipgloss.NewStyle().Foreground(lipgloss.Color("3")).Render("~"),
				e.Path, displayable(e.From), displayable(e.To))
		case message.DiffAdded:
			fmt.Fprintf(&b, "%s %s: %s\n",
				lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render("+"),
				e.Path, displayable(e.To))
		case message.DiffRemoved:
			fmt.Fprintf(&b, "%s %s: %s\n",
				lipgloss.NewStyle().Foreground(lipgloss.Color("1")).Render("-"),
				e.Path, displayable(e.From))
		}
	}
	return b.String()
}

func (m Model) renderDestinations() string {
	var b strings.Builder
	if len(m.detail.Destinations) == 0 {
		return helpStyle.Render("no destination activity recorded")
	}
	b.WriteString(headerStyle.Render(fmt.Sprintf("%-20s %-12s %8s  %s", "DESTINATION", "STATE", "ATTEMPTS", "LAST ERROR"))+ "\n")
	for _, ds := range m.detail.Destinations {
		b.WriteString(fmt.Sprintf("%-20s %-22s %8d  %s\n",
			truncate(ds.DestinationID, 20), stateBadge(ds.State), ds.Attempts, truncate(ds.LastError, 50)))
		if ds.SentAt != nil {
			b.WriteString(helpStyle.Render(fmt.Sprintf("%20s sent %s", "", ds.SentAt.Format("15:04:05.000"))) + "\n")
		}
	}
	return b.String()
}

// scrollWindow slices content to the viewport height at the current scroll
// offset.
func (m Model) scrollWindow(content string) string {
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
	height := max(5, m.height-12)
	offset := m.scroll
	if offset > len(lines)-1 {
		offset = max(0, len(lines)-1)
	}
	end := offset + height
	if end > len(lines) {
		end = len(lines)
	}
	out := strings.Join(lines[offset:end], "\n")
	if end < len(lines) {
		out += "\n" + helpStyle.Render(fmt.Sprintf("… %d more lines (↓)", len(lines)-end))
	}
	return out
}

// displayable makes control characters visible: CR shown as a segment break
// for HL7 payloads, other controls escaped.
func displayable(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	var b strings.Builder
	for _, r := range s {
		if r < 0x20 && r != '\n' && r != '\t' {
			fmt.Fprintf(&b, "\\x%02X", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}
