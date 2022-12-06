package event

import (
	"context"
	"encoding/json"

	"github.com.br/souzarogih/imersao11/internal/usecase"
	"github.com.br/souzarogih/imersao11/pkg/uow"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProcessNewPlayer struct{}

func (p ProcessNewPlayer) Process(ctx context.Context, msg *kafka.Message, uow uow.UowInterface) error {
	var input usecase.AddPlayerInput
	err := json.Unmarshal(msg.Value, &input)
	if err != nil {
		return err
	}
	addNewPlayerUsecase := usecase.NewAddPlayerUseCase(uow)
	err = addNewPlayerUsecase.Execute(ctx, input)
	if err != nil {
		return err
	}
	return nil
}