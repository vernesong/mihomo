package mekya

import (
	"time"

	"github.com/metacubex/mihomo/common/net/deadline"
)

type pipeDeadlines struct {
	read  deadline.PipeDeadline
	write deadline.PipeDeadline
}

func newPipeDeadlines() pipeDeadlines {
	return pipeDeadlines{
		read:  deadline.MakePipeDeadline(),
		write: deadline.MakePipeDeadline(),
	}
}

func (d *pipeDeadlines) SetDeadline(t time.Time) error {
	d.read.Set(t)
	d.write.Set(t)
	return nil
}

func (d *pipeDeadlines) SetReadDeadline(t time.Time) error {
	d.read.Set(t)
	return nil
}

func (d *pipeDeadlines) SetWriteDeadline(t time.Time) error {
	d.write.Set(t)
	return nil
}
