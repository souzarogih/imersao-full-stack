package event

import (
	"context"

	"github.com.br/souzarogih/imersao11/pkg/uow"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProcessEventStrategy interface {
	Process(ctx context.Context, msg *kafka.Message, uow uow.UowInterface) error
}