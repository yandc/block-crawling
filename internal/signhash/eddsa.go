package signhash

import "encoding/json"

type eddsaSignHash struct{}

func (s *eddsaSignHash) Hash(req *SignMessageRequest) (string, error) {
	var msg string
	if err := json.Unmarshal(req.Message, &msg); err != nil {
		return msg, err
	}
	return msg, nil
}

func init() {
	chainTypeHasher["SOL"] = &eddsaSignHash{}
	chainTypeHasher["SUI"] = &eddsaSignHash{}
}
