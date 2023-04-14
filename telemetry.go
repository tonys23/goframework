package goframework

import (
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/integrations/nrmongo"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.mongodb.org/mongo-driver/event"
)

type GoTelemetry struct {
	ProjectName string
	Endpoint    string
	ApiKey      string
}

type agentTelemetry struct {
	agent *newrelic.Application
}

type gfAgentTelemetry interface {
	gin() gin.HandlerFunc
	mongoMonitor() *event.CommandMonitor
	getAgent() *newrelic.Application
}

func NewTelemetry(projectName string, endpoint string, apiKey string) *GoTelemetry {
	return &GoTelemetry{Endpoint: endpoint, ApiKey: apiKey, ProjectName: projectName}
}

func (gt *GoTelemetry) run(gf *GoFramework) {

	if gt.ApiKey != "" {
		app, err := newrelic.NewApplication(
			newrelic.ConfigAppName(gt.ProjectName),
			newrelic.ConfigLicense(gt.ApiKey),
			newrelic.ConfigCodeLevelMetricsEnabled(true),
			newrelic.ConfigDistributedTracerEnabled(true),
		)

		if err != nil {
			panic(err)
		}

		gf.nrApplication = &agentTelemetry{agent: app}
	}
}

func (ag *agentTelemetry) gin() gin.HandlerFunc {
	return nrgin.Middleware(ag.agent)
}

func (ag *agentTelemetry) mongoMonitor() *event.CommandMonitor {
	return nrmongo.NewCommandMonitor(nil)
}

func (ag *agentTelemetry) getAgent() *newrelic.Application {
	return ag.agent
}
