package kafkajobs

import (
	"github.com/roadrunner-server/api/v4/plugins/v2/jobs"
)

type cmd struct {
	cmd  jobs.Command
	pipe string
}

func newCmd(command jobs.Command, pipeline string) *cmd {
	return &cmd{
		cmd:  command,
		pipe: pipeline,
	}
}

func (c *cmd) Command() jobs.Command {
	return c.cmd
}

func (c *cmd) Pipeline() string {
	return c.pipe
}
