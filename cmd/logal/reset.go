package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/CaliLuke/autok-logal/internal/store"
	"github.com/urfave/cli/v3"
)

func newResetCommand() *cli.Command {
	return &cli.Command{
		Name: "reset-db", Usage: "Delete a stopped, disposable Logal database under its ownership lock",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "path", Usage: "Disposable database path"},
			&cli.BoolFlag{Name: "confirm", Usage: "Confirm database deletion"},
		},
		Action: func(_ context.Context, command *cli.Command) error {
			if command.Args().Len() != 0 {
				return errors.New("reset-db does not accept positional arguments")
			}
			if !command.Bool("confirm") {
				return errors.New("reset-db requires --confirm to delete the disposable database")
			}
			if err := store.Reset(command.String("path")); err != nil {
				return err
			}
			_, err := fmt.Fprintf(command.Root().Writer, "Removed disposable Logal database files at %s\n", command.String("path"))
			return err
		},
	}
}
