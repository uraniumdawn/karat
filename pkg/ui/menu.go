// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"fmt"

	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// Menu renders the bottom keybinding bar for the currently active page.
type Menu struct {
	Content *tview.Table
	Flex    *tview.Flex
	Map     *map[string]*[]string
	Colors  *config.ColorConfig
	// current is the page menu last asked for, painted or not, so that Unpin can put back
	// whatever was standing before Pin - including a menu set while the bar was pinned.
	current string
	pinned  bool
}

// Pair holds a keybinding's display key and description.
type Pair struct {
	Key   string
	Value string
}

var keys = map[string]Pair{
	"sel": {
		Key:   "<j/↓,k/↑>",
		Value: "Move",
	},
	"forward": {
		Key:   "<f>",
		Value: "Forward",
	},
	"b/f": {
		Key:   "<b/f>",
		Value: "Backward/Forward",
	},
	"select": {
		Key:   "<Enter>",
		Value: "Select",
	},
	"edit": {
		Key:   "<e>",
		Value: "Edit",
	},
	"res": {
		Key:   "<:>",
		Value: "Resources",
	},
	"opened": {
		Key:   "<C-p>",
		Value: "Opened Pages",
	},
	"search": {
		Key:   "</>",
		Value: "Search",
	},
	"resource_search": {
		Key:   "<:>",
		Value: "Search",
	},
	"dsc": {
		Key:   "<Enter,d>",
		Value: "Details",
	},
	// Clusters is the one list where <Enter> is not "open this": it selects the cluster
	// Karat works against, so only <d> describes it.
	"dsc_key": {
		Key:   "<d>",
		Value: "Details",
	},
	"versions": {
		Key:   "<Enter,d>",
		Value: "Versions",
	},
	"upd": {
		Key:   "<C-u>",
		Value: "Update",
	},
	"auto_upd": {
		Key:   "<a>",
		Value: "Auto-update mode",
	},
	"extra_actions": {
		Key:   "<.>",
		Value: "Extra",
	},
	"auto_upd_tab": {
		Key:   "<Tab>",
		Value: "Set interval",
	},
	"auto_upd_esc": {
		Key:   "<Esc>",
		Value: "Exit mode",
	},
	"create": {
		Key:   "<n>",
		Value: "New Topic",
	},
	"delete_t": {
		Key:   "<C-d>",
		Value: "Delete Topic",
	},
	"toggle_internal": {
		Key:   "<i>",
		Value: "Hide Internal",
	},
	"delete_cg": {
		Key:   "<C-d>",
		Value: "Delete Group",
	},
	"delete_conn": {
		Key:   "<C-d>",
		Value: "Delete Connector",
	},
	"create_conn": {
		Key:   "<n>",
		Value: "New Connector",
	},
	"delete_offsets": {
		Key:   "<C-d>",
		Value: "Delete Offsets",
	},
	"clone_offsets": {
		Key:   "<n>",
		Value: "Clone Offsets",
	},
	"delete_subj": {
		Key:   "<C-d>",
		Value: "Delete Subject",
	},
	"delete_ver": {
		Key:   "<C-d>",
		Value: "Delete Version",
	},
	"edit_topic": {
		Key:   "<e>",
		Value: "Edit Topic",
	},
	"submit_ctrl": {
		Key:   "<C-Enter>",
		Value: "Submit",
	},
	"answer_yes": {
		Key:   "<Y>",
		Value: "Yes",
	},
	// <N> and <Esc> are one action, so they are one entry. The alternative — a single
	// <Y/N/Esc> over Yes/No/Cancel — leaves the reader pairing two lists by position.
	"answer_no": {
		Key:   "<N/Esc>",
		Value: "Cancel",
	},
	"reset_offset": {
		Key:   "<e/E>",
		Value: "Reset Offsets by topic/partition",
	},
	"close": {
		Key:   "<Esc>",
		Value: "Close",
	},
	"close_f1": {
		Key:   "<Esc>/F1",
		Value: "Close",
	},
	"close_f2": {
		Key:   "<Esc>/F2",
		Value: "Close",
	},
	"close_f12": {
		Key:   "<Esc>/F12",
		Value: "Close",
	},
	"config_help": {
		Key:   "<F12>",
		Value: "Config",
	},
	"actions": {
		Key:   "<.>",
		Value: "Actions",
	},
	"task_actions": {
		Key:   "<.>",
		Value: "Task Actions",
	},
	"offsets": {
		Key:   "<o>",
		Value: "Offsets",
	},
	"cancel": {
		Key:   "<Esc>",
		Value: "Cancel",
	},
	"consume_help": {
		Key:   "<F1>",
		Value: "Consume help",
	},
	"consume_editor": {
		Key:   "<C-o>",
		Value: "Edit in editor",
	},
	"consume_now": {
		Key:   "<c>",
		Value: "Consume",
	},
	"consume_history": {
		Key:   "<C-r>",
		Value: "History",
	},
	"history_select": {
		Key:   "<Enter>",
		Value: "Use params",
	},
	"execute_cli": {
		Key:   "<Enter>",
		Value: "Execute CLI command (Beta)",
	},
	"copy_cli": {
		Key:   "<y>",
		Value: "Copy CLI command",
	},
	"terminate_cli": {
		Key:   "<t>",
		Value: "Terminate process",
	},
	"kill_cli": {
		Key:   "<C-k>",
		Value: "Kill process",
	},
	"remove_page": {
		Key:   "<x>",
		Value: "Remove page",
	},
	"stop_consume": {
		Key:   "<t>",
		Value: "Stop consuming",
	},
	"consume_stats": {
		Key:   "<F2>",
		Value: "Consume stats",
	},
	"schema_karat": {
		Key:   "<1>",
		Value: "Karat format",
	},
	"schema_json": {
		Key:   "<2>",
		Value: "JSON format",
	},
	"switch_act": {
		Key:   "<Tab>",
		Value: "Switch action",
	},
	"esc_confirm": {
		Key:   "<Esc>",
		Value: "Confirm and back",
	},
	"hlscroll": {
		Key:   "<h,l>",
		Value: "Scroll Left/Right",
	},
	"sort_2": {
		Key:   "<1/2>",
		Value: "Sort by column",
	},
	"sort_3": {
		Key:   "<1/2/3>",
		Value: "Sort by column",
	},
	"toggle_mode": {
		Key:   "<Tab>",
		Value: "Cycle Mode",
	},
	"clear_filter": {
		Key:   "<Esc>",
		Value: "Clear filter",
	},
	"help": {
		Key:   "<?>",
		Value: "Keys",
	},
	"close_help": {
		Key:   "<Esc>/?",
		Value: "Close",
	},
}

// Page menu identifiers, used as keys into the map passed to NewMenu to look up
// the keybindings shown for the currently active page.
const (
	ConfirmationMenu                    = "ConfirmationMenu"
	ResourcesPageMenu                   = "ResourcesPageMenu"
	OpenedPagesMenu                     = "OpenedPagesMenu"
	ClustersPageMenu                    = "ClustersPageMenu"
	SchemaRegistriesPageMenu            = "SchemaRegistriesPageMenu"
	NodesPageMenu                       = "NodesPageMenu"
	TopicsPageMenu                      = "TopicsPageMenu"
	CopyConsumerGroupPageMenu           = "CopyConsumerGroupPageMenu"
	CopyConnectorOffsetsPageMenu        = "CopyConnectorOffsetsPageMenu"
	ConsumerGroupsPageMenu              = "ConsumerGroupsPageMenu"
	CloneSubjectPageMenu                = "CloneSubjectPageMenu"
	CloneSubjectInputMenu               = "CloneSubjectInputMenu"
	SubjectsPageMenu                    = "SubjectsPageMenu"
	VersionsPageMenu                    = "VersionsPageMenu"
	ConsumerGroupDescribePageMenu       = "ConsumerGroupDescribePageMenu"
	TransactionsPageMenu                = "TransactionsPageMenu"
	TransactionDescribePageMenu         = "TransactionDescribePageMenu"
	ACLsPageMenu                        = "ACLsPageMenu"
	TopicDecriptionPageMenu             = "TopicDescriptionPageMenu"
	TopicProducersPageMenu              = "TopicProducersPageMenu"
	ExtraActionsPageMenu                = "ExtraActionsPageMenu"
	SubjectDecriptionPageMenu           = "SubjectDescriptionPageMenu"
	NodeDecriptionPageMenu              = "NodeDescriptionPageMenu"
	ClusterDescriptionPageMenu          = "ClusterDescriptionPageMenu"
	ConnectorDescriptionPageMenu        = "ConnectorDescriptionPageMenu"
	ConnectorDescriptionPageMenuRunning = "ConnectorDescriptionPageMenuRunning"
	TaskActionsPageMenu                 = "TaskActionsPageMenu"
	ConnectorOffsetsPageMenu            = "ConnectorOffsetsPageMenu"
	CliTemplatesPageMenu                = "CliTemplatesPageMenu"
	CliExecutePageMenu                  = "CliExecutePageMenu"
	ConnectorsPageMenu                  = "ConnectorsPageMenu"
	ConnectorActionsPageMenu            = "ConnectorActionsPageMenu"
	ConnectPageMenu                     = "ConnectPageMenu"
	FindByPageMenu                      = "FindByPageMenu"
	FindSchemaByIDPageMenu              = "FindSchemaByIDPageMenu"
	ConsumeOutputPageMenu               = "ConsumeOutputPageMenu"
	ConsumeParamsPageMenu               = "ConsumeParamsPageMenu"
	ConsumeHistoryPageMenu              = "ConsumeHistoryPageMenu"
	ConsumeHelpPageMenu                 = "ConsumeHelpPageMenu"
	ConsumeStatsPageMenu                = "ConsumeStatsPageMenu"
	ClusterConfigPageMenu               = "ClusterConfigPageMenu"
	AutoUpdateModePageMenu              = "AutoUpdateModePageMenu"
	HelpPageMenu                        = "HelpPageMenu"
)

// NewMenu builds the keybinding bar, pre-rendering the keybinding rows for every
// PageMenu defined in its internal map. Hints for optional columns follow cfg: a disabled
// column takes its sort key with it.
func NewMenu(colors *config.ColorConfig, cfg *config.Config) *Menu {
	table := tview.NewTable().
		SetSelectable(false, false)

	flex := tview.NewFlex().SetDirection(tview.FlexColumn)
	flex.AddItem(table, 0, 1, true)

	// Topics sorts by Name/Partitions/Size, Consumer Groups by Name/State/Lag — the third
	// key disappears with the optional column it sorts.
	topicsSort := "sort_3"
	if !cfg.TopicSizeEnabled() {
		topicsSort = "sort_2"
	}
	groupsSort := "sort_3"
	if !cfg.ConsumerGroupLagEnabled() {
		groupsSort = "sort_2"
	}

	return &Menu{
		Content: table,
		Flex:    flex,
		Map: &map[string]*[]string{
			ConfirmationMenu: {
				"answer_yes",
				"answer_no",
			},
			ResourcesPageMenu: {
				"sel",
				"resource_search",
				"select",
				"close",
			},
			OpenedPagesMenu: {
				"sel",
				"select",
				"search",
				"remove_page",
				"close",
			},
			CliTemplatesPageMenu: {
				"sel",
				"copy_cli",
				"execute_cli",
				"close",
			},
			ClustersPageMenu: {
				"sel",
				"select",
				"toggle_mode",
				"res",
				"dsc_key",
				"config_help",
				"opened",
				"forward",
			},
			SchemaRegistriesPageMenu: {
				"sel",
				"select",
				"res",
				"opened",
				"b/f",
			},
			ConnectPageMenu: {
				"sel",
				"select",
				"res",
				"opened",
				"b/f",
			},
			NodesPageMenu: {
				"sel",
				"res",
				"dsc",
				"upd",
				"opened",
				"b/f",
			},
			TopicsPageMenu: {
				"sel",
				"res",
				"dsc",
				"consume_now",
				topicsSort,
				"toggle_internal",
				"create",
				"delete_t",
				"edit_topic",
				"extra_actions",
				"search",
				"upd",
				"opened",
				"b/f",
			},
			ConsumeParamsPageMenu: {
				"submit_ctrl",
				"cancel",
				"consume_history",
				"consume_editor",
				"consume_help",
			},
			ConsumeHistoryPageMenu: {
				"history_select",
				"submit_ctrl",
				"cancel",
			},
			ConsumeHelpPageMenu: {
				"close_f1",
			},
			ConsumeOutputPageMenu: {
				"stop_consume",
				"consume_stats",
				"remove_page",
				"b/f",
			},
			ConsumeStatsPageMenu: {
				"close_f2",
			},
			ClusterConfigPageMenu: {
				"close_f12",
			},
			AutoUpdateModePageMenu: {
				"auto_upd_tab",
				"auto_upd_esc",
			},
			HelpPageMenu: {
				"close_help",
			},
			CliExecutePageMenu: {
				"terminate_cli",
				"kill_cli",
				"remove_page",
				"b/f",
			},
			ConsumerGroupsPageMenu: {
				"sel",
				"res",
				"dsc",
				groupsSort,
				"delete_cg",
				"extra_actions",
				"search",
				"upd",
				"opened",
				"b/f",
			},
			ConsumerGroupDescribePageMenu: {
				"res",
				"reset_offset",
				"hlscroll",
				"opened",
				"upd",
				"auto_upd",
				"b/f",
			},
			CopyConsumerGroupPageMenu: {
				"submit_ctrl",
				"close",
			},
			TransactionsPageMenu: {
				"sel",
				"res",
				"dsc",
				"sort_2",
				"search",
				"upd",
				"opened",
				"b/f",
			},
			TransactionDescribePageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"auto_upd",
				"b/f",
			},
			ACLsPageMenu: {
				"sel",
				"res",
				"sort_2",
				"search",
				"upd",
				"opened",
				"b/f",
			},
			CloneSubjectPageMenu: {
				"sel",
				"edit",
				"submit_ctrl",
				"close",
			},
			CloneSubjectInputMenu: {
				"esc_confirm",
			},
			SubjectsPageMenu: {
				"sel",
				"versions",
				"res",
				"search",
				"opened",
				"upd",
				"delete_subj",
				"extra_actions",
				"b/f",
			},
			VersionsPageMenu: {
				"sel",
				"res",
				"dsc",
				"opened",
				"upd",
				"delete_ver",
				"b/f",
			},
			ConnectorsPageMenu: {
				"sel",
				"res",
				"dsc",
				"sort_3",
				"actions",
				"edit",
				"create_conn",
				"delete_conn",
				"search",
				"opened",
				"upd",
				"b/f",
			},
			ConnectorDescriptionPageMenu: {
				"res",
				"edit",
				"offsets",
				"hlscroll",
				"opened",
				"upd",
				"b/f",
			},
			ConnectorDescriptionPageMenuRunning: {
				"res",
				"task_actions",
				"edit",
				"offsets",
				"hlscroll",
				"opened",
				"upd",
				"b/f",
			},
			ConnectorOffsetsPageMenu: {
				"upd",
				"delete_offsets",
				"clone_offsets",
				"close",
			},
			TaskActionsPageMenu: {
				"sel",
				"switch_act",
				"submit_ctrl",
				"close",
			},
			ConnectorActionsPageMenu: {
				"switch_act",
				"submit_ctrl",
				"close",
			},
			CopyConnectorOffsetsPageMenu: {
				"submit_ctrl",
				"close",
			},
			TopicDecriptionPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"auto_upd",
				"extra_actions",
				"b/f",
			},
			ExtraActionsPageMenu: {
				"sel",
				"select",
				"close",
			},
			TopicProducersPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"auto_upd",
				"b/f",
			},
			SubjectDecriptionPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"schema_karat",
				"schema_json",
				"b/f",
			},
			NodeDecriptionPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"b/f",
			},
			ClusterDescriptionPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"b/f",
			},
			FindByPageMenu: {
				"submit_ctrl",
				"cancel",
			},
			FindSchemaByIDPageMenu: {
				"submit_ctrl",
				"cancel",
			},
		},
		Colors: colors,
	}
}

// SetMenu redraws the keybinding bar with the entries registered for the given PageMenu. While
// the bar is pinned the menu is remembered but not painted, so that Unpin lands on the page that
// is in front by then rather than on the one that was in front when the bar was pinned.
func (m *Menu) SetMenu(menu string) {
	m.current = menu
	if m.pinned {
		return
	}
	m.render(menu)
}

// Pin paints the given menu and holds the bar on it until Unpin. It is for a standing question,
// which consumes every keypress: what the page underneath advertises would all do nothing, and a
// page rebuilt by a background refresh must not put those bindings back under the question.
//
// ConfirmationMenu is the one it is pinned to. It repeats the [Y/N] the question itself reads,
// because <?> does not open the key reference over a standing question — the bar is the only one
// on screen at that point — and it adds the <Esc> the question does not mention.
func (m *Menu) Pin(menu string) {
	m.pinned = true
	m.render(menu)
}

// Unpin releases the bar and paints the menu last asked for, which is the one belonging to
// whatever page is in front by then.
func (m *Menu) Unpin() {
	m.pinned = false
	m.render(m.current)
}

func (m *Menu) render(menu string) {
	m.Content.Clear()
	if keyBindings, ok := (*m.Map)[menu]; ok {
		row := 0
		col := 0
		maxRowsPerColumn := 3

		for _, binding := range *keyBindings {
			if value, exists := keys[binding]; exists {
				keyColor := m.Colors.Karat.Keybinding.Key
				valueColor := m.Colors.Karat.Keybinding.Value

				// Calculate the current column offset (each column takes 2 cells: key and value)
				colOffset := col * 2

				m.Content.SetCell(
					row,
					colOffset,
					tview.NewTableCell(fmt.Sprintf("[%s]%s", keyColor, value.Key)),
				)
				m.Content.SetCell(
					row,
					colOffset+1,
					tview.NewTableCell(fmt.Sprintf("[%s]%s", valueColor, value.Value)),
				)

				row++

				// If we've reached the max rows per column, move to the next column
				if row >= maxRowsPerColumn {
					row = 0
					col++
				}
			}
		}
	}
}
