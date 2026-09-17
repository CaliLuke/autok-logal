package main

import (
	"errors"
	"fmt"

	"github.com/CaliLuke/autok-logal/internal/store"
	"github.com/spf13/cobra"
)

func newResetCommand() *cobra.Command {
	var path string
	var confirmed bool
	command := &cobra.Command{
		Use:   "reset-db --path PATH --confirm",
		Short: "Delete a stopped, disposable Logal database under its ownership lock",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if !confirmed {
				return errors.New("reset-db requires --confirm to delete the disposable database")
			}
			if err := store.Reset(path); err != nil {
				return err
			}
			_, err := fmt.Fprintf(command.OutOrStdout(), "Removed disposable Logal database files at %s\n", path)
			return err
		},
	}
	command.Flags().StringVar(&path, "path", "", "Disposable database path")
	command.Flags().BoolVar(&confirmed, "confirm", false, "Confirm database deletion")
	return command
}
