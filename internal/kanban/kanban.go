package kanban

import "github.com/google/wire"

var ProviderSet = wire.NewSet(NewMigrateScheduler, NewAggerator, NewTimeMachine, NewRecordSync)
