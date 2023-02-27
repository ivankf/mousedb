// Package help is the help subcommand of the moused command.
package help

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Command displays help for command-line sub-commands.
type Command struct {
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(usage))
	return nil
}

const usage = `
Configure and start MouseDB server.

Usage: moused [[command] [arguments]]

The commands are:
	
	help                 display this help message
	run                  run node with existing configuration
	config               display the default configuration
	version              displays the MouseDB version

"run" is the default command.

Use "moused [command] -help" for more information about a command.
`
