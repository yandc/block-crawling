package kanban

import (
	"fmt"
	"time"
)

// GetShardingTable to return table name that sharding by tx time.
func GetShardingTable(tableName string, txTime int64) string {
	return fmt.Sprintf("%s_%s", tableName, Sharding(txTime))
}

func Sharding(txTime int64) string {
	return time.Unix(txTime, 0).UTC().Format("20060102")
}

func TimeSharding(txTime int64) int64 {
	return txTime - txTime%(24*3600)
}
