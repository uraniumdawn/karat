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
}

// Pair holds a keybinding's display key and description.
type Pair struct {
	Key   string
	Value string
}

var keys = map[string]Pair{
	"sel": {
		Key:   "<j/↓,k/↑>",
		Value: "Selection",
	},
	"hjkl": {
		Key:   "<hjkl>",
		Value: "Move",
	},
	"forward": {
		Key:   "<l>",
		Value: "Forward",
	},
	"backward": {
		Key:   "<h>",
		Value: "Backward",
	},
	"b/f": {
		Key:   "<h/l>",
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
		Key:   "<Ctrl+p>",
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
		Key:   "<d>",
		Value: "Details",
	},
	"upd": {
		Key:   "<Ctrl+u>",
		Value: "Update",
	},
	"auto_upd": {
		Key:   "<g>",
		Value: "Auto-update mode",
	},
	"extra_actions": {
		Key:   "<>>",
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
	"term": {
		Key:   "<e>",
		Value: "Terminating",
	},
	"default": {
		Key:   "<c>",
		Value: "Default",
	},
	"create": {
		Key:   "<c>",
		Value: "Create Topic",
	},
	"delete_t": {
		Key:   "<Ctrl+d>",
		Value: "Delete Topic",
	},
	"toggle_internal": {
		Key:   "<i>",
		Value: "Hide Internal",
	},
	"delete_cg": {
		Key:   "<Ctrl+d>",
		Value: "Delete Group",
	},
	"delete_conn": {
		Key:   "<Ctrl+d>",
		Value: "Delete Connector",
	},
	"delete_offsets": {
		Key:   "<Ctrl+d>",
		Value: "Delete Offsets",
	},
	"copy_offsets": {
		Key:   "<c>",
		Value: "Copy Offsets",
	},
	"edit_topic": {
		Key:   "<e>",
		Value: "Edit Topic",
	},
	"submit_ctrl": {
		Key:   "<Ctrl+Enter>",
		Value: "Submit",
	},
	"reset_offset": {
		Key:   "<o>",
		Value: "Reset Offsets",
	},
	"copy_cgroup": {
		Key:   "<c>",
		Value: "Copy to...",
	},
	"confirm": {
		Key:   "<Ctrl+Enter>",
		Value: "Confirm",
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
		Key:   "<a>",
		Value: "Actions",
	},
	"task_actions": {
		Key:   "<a>",
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
	"cli_commands": {
		Key:   "<t>",
		Value: "CLI commands",
	},
	"consume_help": {
		Key:   "<F1>",
		Value: "Consume help",
	},
	"execute_cli": {
		Key:   "<e>",
		Value: "Execute CLI command (Beta)",
	},
	"copy_cli": {
		Key:   "<c>",
		Value: "Copy CLI command",
	},
	"terminate_cli": {
		Key:   "<t>",
		Value: "Terminate process",
	},
	"kill_cli": {
		Key:   "<Ctrl+k>",
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
	"delete_cli": {
		Key:   "<Ctrl+d>",
		Value: "Remove page",
	},
	"enter": {
		Key:   "<Enter>",
		Value: "Confirm",
	},
	"enter_value": {
		Key:   "<Enter>",
		Value: "Enter Value",
	},
	"esc": {
		Key:   "<Esc>",
		Value: "Back",
	},
	"switch_act": {
		Key:   "<Tab>",
		Value: "Switch action",
	},
	"esc_confirm": {
		Key:   "<Esc>",
		Value: "Confirm and back",
	},
	"esc_confirm_opened": {
		Key:   "<Esc, Enter>",
		Value: "Confirm and back",
	},
	"hlscroll": {
		Key:   "<H,L>",
		Value: "Scroll Left/Right",
	},
	"batch_set_st": {
		Key:   "<Enter>",
		Value: "Select strategy",
	},
	"q": {
		Key:   "<q>",
		Value: "",
	},
	"sort_2": {
		Key:   "<1/2>",
		Value: "Sort by column",
	},
	"sort_3": {
		Key:   "<1/2/3>",
		Value: "Sort by column",
	},
	"find": {
		Key:   "<f>",
		Value: "Find By",
	},
	"toggle_mode": {
		Key:   "<Tab>",
		Value: "Toggle Mode",
	},
}

// Page menu identifiers, used as keys into the map passed to NewMenu to look up
// the keybindings shown for the currently active page.
const (
	ResourcesPageMenu                   = "ResourcesPageMenu"
	OpenedPagesMenu                     = "OpenedPagesMenu"
	ClustersPageMenu                    = "ClustersPageMenu"
	SchemaRegistriesPageMenu            = "SchemaRegistriesPageMenu"
	NodesPageMenu                       = "NodesPageMenu"
	TopicsPageMenu                      = "TopicsPageMenu"
	CreateTopicPageMenu                 = "CreateTopicPageMenu"
	CreateTopicInputMenu                = "CreateTopicInputMenu"
	DeleteTopicPageMenu                 = "DeleteTopicPageMenu"
	DeleteConsumerGroupPageMenu         = "DeleteConsumerGroupPageMenu"
	EditTopicPageMenu                   = "EditTopicPageMenu"
	EditTopicInputMenu                  = "EditTopicInputMenu"
	ResetOffsetPageMenu                 = "ResetOffsetPageMenu"
	CopyConsumerGroupPageMenu           = "CopyConsumerGroupPageMenu"
	CopyConnectorOffsetsPageMenu        = "CopyConnectorOffsetsPageMenu"
	ConsumerGroupsPageMenu              = "ConsumerGroupsPageMenu"
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
	ConnectorDescriptionPageMenu        = "ConnectorDescriptionPageMenu"
	ConnectorDescriptionPageMenuRunning = "ConnectorDescriptionPageMenuRunning"
	TaskActionsPageMenu                 = "TaskActionsPageMenu"
	ConnectorOffsetsPageMenu            = "ConnectorOffsetsPageMenu"
	CliTemplatesPageMenu                = "CliTemplatesPageMenu"
	CliExecutePageMenu                  = "CliExecutePageMenu"
	ConnectorsPageMenu                  = "ConnectorsPageMenu"
	ConnectorConfigEditPageMenu         = "ConnectorConfigEditPageMenu"
	ConnectorActionsPageMenu            = "ConnectorActionsPageMenu"
	DeleteConnectorPageMenu             = "DeleteConnectorPageMenu"
	DeleteConnectorOffsetsPageMenu      = "DeleteConnectorOffsetsPageMenu"
	ConnectPageMenu                     = "ConnectPageMenu"
	FindByPageMenu                      = "FindByPageMenu"
	ConsumeOutputPageMenu               = "ConsumeOutputPageMenu"
	ConsumeParamsPageMenu               = "ConsumeParamsPageMenu"
	ConsumeHelpPageMenu                 = "ConsumeHelpPageMenu"
	ConsumeStatsPageMenu                = "ConsumeStatsPageMenu"
	ClusterConfigPageMenu               = "ClusterConfigPageMenu"
	AutoUpdateModePageMenu              = "AutoUpdateModePageMenu"
)

// NewMenu builds the keybinding bar, pre-rendering the keybinding rows for every
// PageMenu defined in its internal map.
func NewMenu(colors *config.ColorConfig) *Menu {
	table := tview.NewTable().
		SetSelectable(false, false)

	flex := tview.NewFlex().SetDirection(tview.FlexColumn)
	flex.AddItem(table, 0, 1, true)

	return &Menu{
		Content: table,
		Flex:    flex,
		Map: &map[string]*[]string{
			ResourcesPageMenu: {
				"sel",
				"resource_search",
				"select",
				"close",
			},
			OpenedPagesMenu: {
				"sel",
				"search",
				"remove_page",
				"esc_confirm_opened",
			},
			CreateTopicPageMenu: {
				"sel",
				"edit",
				"submit_ctrl",
				"default",
				"close",
			},
			CreateTopicInputMenu: {
				"esc_confirm",
			},
			EditTopicPageMenu: {
				"sel",
				"edit",
				"submit_ctrl",
				"close",
			},
			EditTopicInputMenu: {
				"esc_confirm",
			},
			ResetOffsetPageMenu: {
				"sel",
				"batch_set_st",
				"submit_ctrl",
				"close",
			},
			DeleteTopicPageMenu: {
				"confirm",
				"cancel",
			},
			DeleteConsumerGroupPageMenu: {
				"confirm",
				"cancel",
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
				"dsc",
				"config_help",
				"upd",
				"opened",
				"forward",
			},
			SchemaRegistriesPageMenu: {
				"sel",
				"select",
				"toggle_mode",
				"res",
				"opened",
				"b/f",
			},
			ConnectPageMenu: {
				"sel",
				"select",
				"toggle_mode",
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
				"sort_2",
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
				"consume_help",
			},
			ConsumeHelpPageMenu: {
				"close_f1",
			},
			ConsumeOutputPageMenu: {
				"stop_consume",
				"consume_stats",
				"delete_cli",
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
			CliExecutePageMenu: {
				"terminate_cli",
				"kill_cli",
				"b/f",
			},
			ConsumerGroupsPageMenu: {
				"sel",
				"res",
				"dsc",
				"sort_2",
				"delete_cg",
				"find",
				"search",
				"upd",
				"opened",
				"b/f",
			},
			ConsumerGroupDescribePageMenu: {
				"res",
				"reset_offset",
				"copy_cgroup",
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
			SubjectsPageMenu: {
				"sel",
				"select",
				"res",
				"search",
				"opened",
				"upd",
				"b/f",
			},
			VersionsPageMenu: {
				"sel",
				"res",
				"dsc",
				"opened",
				"upd",
				"b/f",
			},
			ConnectorsPageMenu: {
				"sel",
				"res",
				"dsc",
				"sort_3",
				"actions",
				"edit",
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
				"hjkl",
				"upd",
				"delete_offsets",
				"copy_offsets",
				"close",
			},
			TaskActionsPageMenu: {
				"sel",
				"switch_act",
				"submit_ctrl",
				"close",
			},
			ConnectorConfigEditPageMenu: {
				"submit_ctrl",
				"cancel",
			},
			ConnectorActionsPageMenu: {
				"switch_act",
				"submit_ctrl",
				"close",
			},
			DeleteConnectorPageMenu: {
				"confirm",
				"cancel",
			},
			DeleteConnectorOffsetsPageMenu: {
				"confirm",
				"cancel",
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
				"b/f",
			},
			NodeDecriptionPageMenu: {
				"res",
				"hlscroll",
				"opened",
				"upd",
				"b/f",
			},
			FindByPageMenu: {
				"sel",
				"enter_value",
				"esc",
			},
		},
		Colors: colors,
	}
}

// SetMenu redraws the keybinding bar with the entries registered for the given PageMenu.
func (m *Menu) SetMenu(menu string) {
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
