package usecase

import (
	"context"

	"github.com.br/souzarogih/imersao11/internal/domain/entity"
	"github.com.br/souzarogih/imersao11/internal/domain/repository"
	"github.com.br/souzarogih/imersao11/pkg/uow"
)

type AddMyTeanInput struct {
	ID    string
	Name  string
	Score string
}

type AddMyTeamUseCase struct {
	Uow uow.UowInterface
}

func (a *AddMyTeamUseCase) Execute(ctx context.Context, input AddMyTeanInput) error {
	myTeamRepository := a.getMyTeamRepository(ctx)
	myTeam := entity.NewMyTeam(input.ID, input.Name)
	err := myTeamRepository.Create(ctx, myTeam)
	if err != nil {
		return err
	}
	return a.Uow.CommitOrRollback()
}

func (a *AddMyTeamUseCase) getMyTeamRepository(ctx context.Context) repository.MyTeamRepositoryInterface {
	myTeamRepository, err := a.Uow.GetRepository(ctx, "MyTeamRepository")
	if err != nil {
		panic(err)
	}
	return myTeamRepository.(repository.MyTeamRepositoryInterface)
}