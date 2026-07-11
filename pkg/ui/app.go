// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package ui provides the terminal user interface for the karat application.
package ui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/patrickmn/go-cache"
	"github.com/rivo/tview"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/client"
	"github.com/uraniumdawn/karat/pkg/config"
	"github.com/uraniumdawn/karat/pkg/connect"
	"github.com/uraniumdawn/karat/pkg/franz"
	"github.com/uraniumdawn/karat/pkg/schemaregistry"
	"github.com/uraniumdawn/karat/pkg/util"
)

// Version is the application version injected at startup via main.
var Version = ""

const (
	Resources              = "Resources"
	ExtraActions           = "Extra Actions"
	Clusters               = "Clusters"
	SchemaRegistries       = "Schema-registries"
	Topics                 = "Topics"
	Topic                  = "Topic"
	Producers              = "Producers"
	Nodes                  = "Nodes"
	Node                   = "Node"
	ConsumerGroups         = "Consumer groups"
	ConsumerGroup          = "Consumer group"
	Transactions           = "Transactions"
	Transaction            = "Transaction"
	ACLs                   = "ACLs"
	Subjects               = "Subjects"
	CloneSubject           = "Clone Subject"
	DeleteSubject          = "Delete Subject"
	DeleteSubjectVersion   = "Delete Subject Version"
	Connectors             = "Connectors"
	Connect                = "Connect"
	OpenedPages            = "Opened pages"
	CreateTopic            = "Create Topic"
	CloneTopic             = "Clone Topic"
	RecreateTopic          = "Recreate Topic"
	DeleteTopic            = "Delete Topic"
	DeleteConsumerGroup    = "Delete Consumer Group"
	EditTopic              = "Edit Topic"
	ResetOffset            = "Reset Offset"
	CopyConsumerGroup      = "Copy Consumer Group"
	ConnectorConfigConfirm = "Connector Config Confirm"
	CreateConnector        = "Create Connector"
	ConnectorActions       = "Connector Actions"
	TaskActions            = "Task Actions"
	ConnectorOffsets       = "Connector Offsets"
	DeleteConnector        = "Delete Connector"
	DeleteConnectorOffsets = "Delete Connector Offsets"
	CopyConnectorOffsets   = "Copy Connector Offsets"
	CliTemplates           = "CLI Templates"
	FindBy                 = "Find By"
	FindSchemaByID         = "Find Schema By ID"
	ConsumeOutput          = "Consume Output"
	ConsumeParams          = "Consume Params"
	ConsumeHelp            = "Consume Help"
	ConsumeStats           = "Consume Stats"
	ClusterConfig          = "Cluster Config"
)

type App struct {
	*tview.Application
	Layout                *Layout
	Cache                 *cache.Cache
	Clusters              map[string]*config.ClusterConfig
	SchemaRegistries      map[string]*config.SchemaRegistryConfig
	Connects              map[string]*config.ConnectConfig
	KafkaClients          map[string]*client.Client
	FranzClients          map[string]*franz.Client
	SchemaRegistryClients map[string]*schemaregistry.Client
	ConnectClients        map[string]*connect.Client
	Selected              Selected
	Config                *config.Config
	Colors                *config.ColorConfig
	CurrentFilters        map[string]string // pageName -> filter text for search preservation
	HideInternalTopics    bool              // hide __*, *-changelog, *-repartition topics
	InternalTopicPatterns []*regexp.Regexp  // extra user-defined patterns for internal topics
	cgroupPrevLag         map[string]map[string]int64
	ResourcesSearchInput  *tview.InputField
	autoUpdate            map[string]*autoUpdateEntry
	autoUpdateMu          sync.Mutex
	autoUpdateMode        bool
	autoUpdatePageKey     string
	LatestVersion         string
}

type Selected struct {
	Cluster        *config.ClusterConfig
	SchemaRegistry *config.SchemaRegistryConfig
	Connect        *config.ConnectConfig
}

func (app *App) GetCurrentKafkaClient() *client.Client {
	return app.KafkaClients[app.Selected.Cluster.Name]
}

// GetCurrentFranzClient returns the franz-go client for the selected cluster, or nil
// if it could not be created (e.g. unsupported security configuration).
func (app *App) GetCurrentFranzClient() *franz.Client {
	return app.FranzClients[app.Selected.Cluster.Name]
}

// IsCurrentClusterReadOnly reports whether the selected cluster is in read-only mode.
func (app *App) IsCurrentClusterReadOnly() bool {
	return app.Selected.Cluster != nil && app.Selected.Cluster.IsReadOnly()
}

// IsCurrentSchemaRegistryReadOnly reports whether the selected schema registry is in read-only mode.
func (app *App) IsCurrentSchemaRegistryReadOnly() bool {
	return app.Selected.SchemaRegistry != nil && app.Selected.SchemaRegistry.IsReadOnly()
}

// IsCurrentConnectReadOnly reports whether the selected Connect cluster is in read-only mode.
func (app *App) IsCurrentConnectReadOnly() bool {
	return app.Selected.Connect != nil && app.Selected.Connect.IsReadOnly()
}

func (app *App) GetCurrentSchemaRegistryClient() *schemaregistry.Client {
	if app.Selected.SchemaRegistry == nil {
		return nil
	}
	return app.SchemaRegistryClients[app.Selected.SchemaRegistry.Name]
}

func (app *App) GetCurrentConnectClient() *connect.Client {
	if app.Selected.Connect == nil {
		return nil
	}
	return app.ConnectClients[app.Selected.Connect.Name]
}

// versionHintText returns the StatusHint display text, including an update arrow when a newer version is known.
func (app *App) versionHintText() string {
	if app.LatestVersion != "" {
		return fmt.Sprintf(" κεράτιον v%s [yellow]→ v%s[-] ", Version, app.LatestVersion)
	}
	return " κεράτιον v" + Version + " "
}

// versionHintDisplayWidth returns the visual width of the hint text (markup stripped).
func (app *App) versionHintDisplayWidth() int {
	if app.LatestVersion != "" {
		return len([]rune(fmt.Sprintf(" κεράτιον v%s → v%s ", Version, app.LatestVersion)))
	}
	return len([]rune(" κεράτιον v" + Version + " "))
}

// resizeStatusHint updates the StatusBar item width to fit the current hint text.
func (app *App) resizeStatusHint() {
	app.Layout.StatusBar.ResizeItem(app.Layout.StatusHint, app.versionHintDisplayWidth(), 0)
}

// setVersionUpdate stores the latest version and updates the status hint in the UI.
func (app *App) setVersionUpdate(latest string) {
	app.QueueUpdateDraw(func() {
		app.LatestVersion = latest
		app.Layout.StatusHint.SetText(app.versionHintText())
		app.resizeStatusHint()
	})
}

func NewApp() *App {
	// Save real stderr before InitLogger redirects os.Stderr to the log file.
	stderr := os.Stderr
	InitLogger()

	fatal := func(msg string, err error) {
		_, _ = fmt.Fprintf(stderr, "karat: %s: %v\n", msg, err)
		os.Exit(1)
	}

	cfg, err := config.LoadAppConfig()
	if err != nil {
		fatal("failed to load config", err)
	}

	colors, err := config.LoadColorConfig(cfg.Karat.Style)
	if err != nil {
		fatal("failed to load style", err)
	}

	var internalTopicPatterns []*regexp.Regexp
	for _, pattern := range cfg.Karat.UI.InternalTopicPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Warn().Err(err).Str("pattern", pattern).
				Msg("invalid internal_topic_patterns regex, skipping")
			continue
		}
		internalTopicPatterns = append(internalTopicPatterns, re)
	}

	app := &App{
		Application:           tview.NewApplication(),
		Cache:                 cache.New(5*time.Minute, 10*time.Minute),
		Clusters:              util.ToClustersMap(cfg),
		SchemaRegistries:      util.ToSchemaRegistryMap(cfg),
		Connects:              util.ToConnectMap(cfg),
		KafkaClients:          make(map[string]*client.Client),
		FranzClients:          make(map[string]*franz.Client),
		SchemaRegistryClients: make(map[string]*schemaregistry.Client),
		ConnectClients:        make(map[string]*connect.Client),
		Config:                cfg,
		Colors:                colors,
		CurrentFilters:        make(map[string]string),
		InternalTopicPatterns: internalTopicPatterns,
		cgroupPrevLag:         make(map[string]map[string]int64),
		autoUpdate:            make(map[string]*autoUpdateEntry),
	}

	return app
}

func InitLogger() {
	zerolog.TimeFieldFormat = time.RFC3339

	logFilePath := filepath.Join(os.Getenv("HOME"), ".config", "karat", "karat.log")
	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		fmt.Printf("failed to create log directory, %s\n", err.Error())
	}

	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Printf("failed to open log file, %s\n", err.Error())
		os.Exit(1)
	}

	// Use ConsoleWriter for human-readable, canonical log format
	consoleWriter := zerolog.ConsoleWriter{
		Out:        file,
		TimeFormat: time.RFC3339,
		NoColor:    true, // Set to true if colors are not desired in log file
	}

	os.Stderr = file
	os.Stdout = file

	// Add caller information (file and line number) to all log entries
	log.Logger = log.Output(consoleWriter).With().Caller().Logger()
}

func (app *App) Run() {
	app.ApplyColors()
	ctx, cancel := context.WithCancel(context.Background())

	app.RunResourcesEventHandler(ctx, ResourcesChannel)
	app.RunStatusLineHandler(ctx, StatusLineCh)
	app.RunClusterEventHandler(ctx, ClustersChannel)
	app.RunSchemaRegistriesEventHandler(ctx, SchemaRegistriesChannel)
	app.RunConnectEventHandler(ctx, ConnectChannel)
	app.RunNodesEventHandler(ctx, NodesChannel)
	app.RunTopicsEventHandler(ctx, TopicsChannel)
	app.RunCgroupsEventHandler(ctx, CgroupsChannel)
	app.RunTransactionsEventHandler(ctx, TransactionsChannel)
	app.RunACLsEventHandler(ctx, ACLsChannel)
	app.RunSubjectsEventHandler(ctx, SubjectsChannel)
	app.RunConnectorsEventHandler(ctx, ConnectorsChannel)
	app.RunVersionChecker(ctx)

	registry := NewPagesRegistry(app.Colors)
	hasSR := len(app.Config.Karat.SchemaRegistries) > 0
	hasConnect := len(app.Config.Karat.Connect) > 0
	app.Layout = NewLayout(registry, app.Colors, app.Config, hasSR, hasConnect)

	for _, c := range app.Config.Karat.Clusters {
		if c.Selected {
			app.SelectCluster(c, false)
			break // Assuming only one can be selected
		}
	}

	for _, sr := range app.Config.Karat.SchemaRegistries {
		if sr.Selected {
			app.SelectSchemaRegistry(sr, false)
			break // Assuming only one can be selected
		}
	}

	for _, cn := range app.Config.Karat.Connect {
		if cn.Selected {
			app.SelectConnect(cn, false)
			break
		}
	}

	Publish(ClustersChannel, GetClustersEventType, Payload{nil, false})
	app.Layout.SetSelected(app.Selected.Cluster, app.Selected.SchemaRegistry, app.Selected.Connect)

	resourcesPage := app.NewResourcesPage()
	app.Layout.PagesRegistry.UI.Pages.AddPage(Resources, resourcesPage, true, false)
	app.Layout.PagesRegistry.UI.Pages.AddPage(
		OpenedPages,
		app.Layout.PagesRegistry.UI.Main,
		true,
		false,
	)
	app.Layout.PagesRegistry.UI.Pages.ShowPage(Clusters)
	app.Layout.Menu.SetMenu(ClustersPageMenu)
	app.Layout.PagesRegistry.UI.FilteredPages.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)

	app.OpenPagesKeyHandler(app.Layout.PagesRegistry.UI.FilteredPages)
	app.MainOperationKeyHandler()

	err := app.SetRoot(app.Layout.Content, true).Run()
	if err != nil {
		log.Error().Err(err).Msg("failed application execution")
	}
	cancel()

	for _, fc := range app.FranzClients {
		fc.Close()
	}

	log.Info().Msg("application terminated")
}

func (app *App) ApplyColors() {
	tview.Styles = tview.Theme{
		PrimitiveBackgroundColor:    tcell.GetColor(app.Colors.Karat.Background),
		ContrastBackgroundColor:     tview.Styles.ContrastBackgroundColor,
		MoreContrastBackgroundColor: tview.Styles.MoreContrastBackgroundColor,
		BorderColor:                 tcell.GetColor(app.Colors.Karat.Border),
		TitleColor:                  tcell.GetColor(app.Colors.Karat.Title),
		GraphicsColor:               tview.Styles.GraphicsColor,
		PrimaryTextColor:            tcell.GetColor(app.Colors.Karat.Foreground),
		SecondaryTextColor:          tview.Styles.SecondaryTextColor,
		TertiaryTextColor:           tview.Styles.TertiaryTextColor,
		InverseTextColor:            tview.Styles.InverseTextColor,
		ContrastSecondaryTextColor:  tview.Styles.ContrastSecondaryTextColor,
	}
}

func (app *App) isClusterSelected(selected Selected) bool {
	return selected.Cluster != nil
}

func (app *App) isSchemaRegistrySelected(selected Selected) bool {
	return selected.SchemaRegistry != nil
}

func (app *App) isConnectSelected(selected Selected) bool {
	return selected.Connect != nil
}

// requireSelection sends msg as a status message and returns false if selected is false.
// Callers use the return value to decide whether to abort the current operation.
func (app *App) requireSelection(selected bool, msg string) bool {
	if !selected {
		SendStatusWithDefaultTTL(msg)
		return false
	}
	return true
}

func (app *App) SelectCluster(cluster *config.ClusterConfig, save bool) {
	app.StopAllAutoUpdates()
	if save {
		for _, c := range app.Config.Karat.Clusters {
			c.Selected = c.Name == cluster.Name
		}
		if err := app.Config.Save(); err != nil {
			log.Error().Err(err).Msg("failed to save config after cluster selection")
		}
	}

	app.Selected.Cluster = cluster
	app.Layout.SetSelected(app.Selected.Cluster, app.Selected.SchemaRegistry, app.Selected.Connect)

	_, exists := app.KafkaClients[cluster.Name]
	if !exists {
		var err error
		timeout := app.Config.GetAPICallTimeout()
		newClient, err := client.NewClient(cluster, timeout, app.Config.GetMaxConcurrency())
		if err != nil {
			log.Error().Err(err).Msg("failed to create admin client")
			os.Exit(1)
		}
		app.KafkaClients[cluster.Name] = newClient
	}

	if _, fcExists := app.FranzClients[cluster.Name]; !fcExists {
		newFranzClient, err := franz.NewClient(cluster, app.Config.GetAPICallTimeout())
		if err != nil {
			log.Warn().Err(err).Str("cluster", cluster.Name).
				Msg("failed to create franz-go client, topic size will be estimated")
		} else {
			app.FranzClients[cluster.Name] = newFranzClient
		}
	}
}

func (app *App) SelectSchemaRegistry(sr *config.SchemaRegistryConfig, save bool) {
	if save {
		for _, r := range app.Config.Karat.SchemaRegistries {
			r.Selected = r.Name == sr.Name
		}
		if err := app.Config.Save(); err != nil {
			log.Error().Err(err).Msg("failed to save config after schema registry selection")
		}
	}

	app.Selected.SchemaRegistry = sr
	app.Layout.SetSelected(app.Selected.Cluster, app.Selected.SchemaRegistry, app.Selected.Connect)

	_, exists := app.SchemaRegistryClients[sr.Name]
	if !exists {
		var err error
		newClient, err := schemaregistry.NewSchemaRegistryClient(sr)
		if err != nil {
			log.Error().Err(err).Msg("failed to create admin client")
			os.Exit(1)
		}
		app.SchemaRegistryClients[sr.Name] = newClient
	}
}

func (app *App) SelectConnect(cfg *config.ConnectConfig, save bool) {
	if save {
		for _, c := range app.Config.Karat.Connect {
			c.Selected = c.Name == cfg.Name
		}
		if err := app.Config.Save(); err != nil {
			log.Error().Err(err).Msg("failed to save config after connect selection")
		}
	}

	app.Selected.Connect = cfg
	app.Layout.SetSelected(app.Selected.Cluster, app.Selected.SchemaRegistry, app.Selected.Connect)

	_, exists := app.ConnectClients[cfg.Name]
	if !exists {
		newClient, err := connect.NewClient(cfg, app.Config.GetAPICallTimeout())
		if err != nil {
			log.Error().Err(err).Msg("failed to create connect client")
			return
		}
		app.ConnectClients[cfg.Name] = newClient
	}
}

func (app *App) NewDescription(title string) *tview.TextView {
	desc := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true).
		SetWrap(false)
	desc.
		SetBorder(true).
		SetBorderPadding(0, 0, 1, 0).
		SetTitle(title)
	desc.SetTextColor(tcell.GetColor(app.Colors.Karat.Foreground))
	return desc
}

// WithHScroll wraps an input capture handler to add H/L horizontal scrolling to a TextView.
func (app *App) WithHScroll(
	desc *tview.TextView,
	handler func(*tcell.EventKey) *tcell.EventKey,
) func(*tcell.EventKey) *tcell.EventKey {
	return func(event *tcell.EventKey) *tcell.EventKey {
		row, col := desc.GetScrollOffset()
		if IsKey(event, 'H') {
			if col > 0 {
				desc.ScrollTo(row, col-5)
			}
			return nil
		}
		if IsKey(event, 'L') {
			desc.ScrollTo(row, col+5)
			return nil
		}
		return handler(event)
	}
}

// ClearCurrentFilter clears the saved filter for the current page.
func (app *App) ClearCurrentFilter() {
	currentPage, _ := app.Layout.PagesRegistry.UI.Pages.GetFrontPage()
	delete(app.CurrentFilters, currentPage)

	// Also clear the search input if it exists
	if search, exists := app.Layout.Search[currentPage]; exists {
		search.SetText("")
	}
}

// ClearFilterForPage clears the saved filter for a specific page.
func (app *App) ClearFilterForPage(pageName string) {
	delete(app.CurrentFilters, pageName)
}
