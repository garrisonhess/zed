package index

import (
	"errors"
	"flag"
	"fmt"

	"github.com/brimdata/zed/cli/lakecli"
	zedlake "github.com/brimdata/zed/cmd/zed/lake"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/pkg/rlimit"
)

var Drop = &charm.Spec{
	Name:  "drop",
	Usage: "drop [-R root] [options] id... ",
	Short: "drop rule from a lake index",
	New:   NewDrop,
}

type DropCommand struct {
	lake *zedlake.Command
}

func NewDrop(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &DropCommand{lake: parent.(*Command).Command}
	return c, nil
}

func (c *DropCommand) Run(args []string) error {
	ctx, cleanup, err := c.lake.Init()
	if err != nil {
		return err
	}
	defer cleanup()
	if len(args) == 0 {
		return errors.New("must specify one or more index tags")
	}
	if _, err := rlimit.RaiseOpenFilesLimit(); err != nil {
		return err
	}
	ids, err := lakecli.ParseIDs(args)
	if err != nil {
		return err
	}
	root, err := c.lake.Open(ctx)
	if err != nil {
		return err
	}
	indices, err := root.DeleteIndices(ctx, ids)
	if err != nil {
		return err
	}
	if !c.lake.Quiet() {
		for _, index := range indices {
			fmt.Printf("%s dropped\n", index.ID)
		}
	}
	return nil
}
