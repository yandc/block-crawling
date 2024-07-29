package ethereum

import (
	"math/big"

	"github.com/shopspring/decimal"
)

// https://github.com/MesonFi/meson-contracts-solidity/blob/7283d390e2745ebf0d80f4773e84e9de296da7a7/contracts/Pools/MesonPools.sol#L254
func extractMesonFiDirectReleaseAmount(methodId string, data []byte) (*big.Int, bool) {
	if methodId != "ab115fd8" || len(data) != 32 {
		return new(big.Int), false
	}
	encodedAmount := new(big.Int).SetBytes(data)
	amount := new(big.Int).Sub(_amountFrom(encodedAmount), _feeForLp(encodedAmount))
	// Need test
	// amount = new(big.Int).Sub(amount, _amountForCoreTokenFrom(encodedAmount))
	amount = amount.Sub(amount, _amountToShare(encodedAmount))
	amount = amount.Sub(amount, _serviceFee(encodedAmount))
	return amount.Mul(amount, big.NewInt(1000000000000)), true // * 1e12
}

func _amountFrom(encodedSwap *big.Int) *big.Int {
	amountFrom := new(big.Int).Rsh(encodedSwap, 208)
	return new(big.Int).And(amountFrom, big.NewInt(0xFFFFFFFFFF))
}

func _feeForLp(encodedSwap *big.Int) *big.Int {
	feeForLp := new(big.Int).Rsh(encodedSwap, 88)
	return new(big.Int).And(feeForLp, big.NewInt(0xFFFFFFFFFF))
}

func _amountForCoreTokenFrom(encodedSwap *big.Int) *big.Int {
	if _swapForCoreToken(encodedSwap) {
		d := new(big.Int).Rsh(encodedSwap, 160)
		d = d.And(d, big.NewInt(0xFFFF))
		if d.Cmp(big.NewInt(0xFFFF)) == 0 {
			amount := _amountFrom(encodedSwap)
			amount = amount.Sub(amount, _feeForLp(encodedSwap))
			if !_feeWaived(encodedSwap) {
				amount = amount.Sub(amount, _serviceFee(encodedSwap))
			}

			return amount
		}
		// _decompressFixedPrecision(d) * 1e3
		p := _decompressFixedPrecision(d)
		return p.Mul(p, big.NewInt(1000))
	}
	return new(big.Int)
}

func _swapForCoreToken(encodedSwap *big.Int) bool {
	// 0x8410000000000000000000000000000000000000000000000000
	x841, _ := decimal.NewFromString("212216255469952903263760368819804786195588432866440078811987968")
	x841Big := x841.BigInt()
	return (_outTokenIndexFrom(encodedSwap) < 191) &&
		(new(big.Int).And(encodedSwap, x841Big).Cmp(x841Big) == 0)
}

func _outTokenIndexFrom(encodedSwap *big.Int) uint8 {
	return uint8(new(big.Int).Rsh(encodedSwap, 24).Int64())
}

func _inTokenIndexFrom(encodedSwap *big.Int) uint8 {
	return uint8(encodedSwap.Int64())
}

func _feeWaived(encodedSwap *big.Int) bool {
	// 0x4000000000000000000000000000000000000000000000000000
	x40, _ := decimal.NewFromString("102844034832575377634685573909834406561420991602098741459288064")
	x40Big := x40.BigInt()
	return new(big.Int).And(encodedSwap, x40Big).Cmp(big.NewInt(0)) > 0
}

const (
	SERVICE_FEE_RATE        int64 = 5       // service fee = 5 / 10000 = 0.05%
	SERVICE_FEE_MINIMUM     int64 = 500_000 // min $0.5
	SERVICE_FEE_MINIMUM_ETH int64 = 500     // min 0.0005 ETH (or SOL)
	SERVICE_FEE_MINIMUM_BNB int64 = 5000    // min 0.005 BNB
	SERVICE_FEE_MINIMUM_BTC int64 = 10      // min 0.00001 BTC
)

func _serviceFee(encodedSwap *big.Int) *big.Int {
	tokenIndex := _inTokenIndexFrom(encodedSwap)
	minFee := new(big.Int)
	if tokenIndex >= 252 {
		minFee = big.NewInt(SERVICE_FEE_MINIMUM_ETH)
	} else if tokenIndex >= 248 {
		minFee = big.NewInt(SERVICE_FEE_MINIMUM_BNB)
	} else if tokenIndex >= 244 {
		minFee = big.NewInt(SERVICE_FEE_MINIMUM_ETH)
	} else if tokenIndex >= 240 {
		minFee = big.NewInt(SERVICE_FEE_MINIMUM_BTC)
	} else {
		minFee = big.NewInt(SERVICE_FEE_MINIMUM)
	}
	// Default to `serviceFee` = 0.05% * `amount`
	// 	fee := _amountFrom(encodedSwap) * SERVICE_FEE_RATE / 10000
	fee := _amountFrom(encodedSwap)
	fee = fee.Mul(fee, big.NewInt(SERVICE_FEE_RATE))
	fee = fee.Div(fee, big.NewInt(10000))
	if fee.Cmp(minFee) > 0 {
		return fee
	}
	return minFee
}

func _decompressFixedPrecision(d *big.Int) *big.Int {
	if d.Cmp(big.NewInt(1000)) <= 0 {
		return d
	}
	// ((d-1000)%9000 + 1000) * 10 ** ((d - 1000) / 9000)
	du := new(big.Int).Mul(d, big.NewInt(1000))
	du = du.Mod(du, big.NewInt(9000))
	du = du.Add(du, big.NewInt(1000))
	du = du.Mul(du, big.NewInt(10))
	dp := new(big.Int).Sub(d, big.NewInt(1000))
	dp = dp.Div(dp, big.NewInt(9000))
	return decimal.NewFromBigInt(du, 0).Pow(decimal.NewFromBigInt(dp, 0)).BigInt()
}

func _amountToShare(encodedSwap *big.Int) *big.Int {
	if _shareWithPartner(encodedSwap) {
		a := new(big.Int).Rsh(encodedSwap, 160)
		a = a.And(a, big.NewInt(0xFFFF))
		return _decompressFixedPrecision(a)
	}
	return new(big.Int)
}

func _shareWithPartner(encodedSwap *big.Int) bool {
	// 0x0600000000000000000000000000000000000000000000000000
	x060, _ := decimal.NewFromString("9641628265553941653251772554046975615133217962696757011808256")
	x060Big := x060.BigInt()
	// 0x0200000000000000000000000000000000000000000000000000
	x020, _ := decimal.NewFromString("3213876088517980551083924184682325205044405987565585670602752")
	x020Big := x020.BigInt()
	return !_willTransferToContract(encodedSwap) && (new(big.Int).And(encodedSwap, x060Big).Cmp(x020Big) == 0)
}

func _willTransferToContract(encodedSwap *big.Int) bool {
	// 0x8000000000000000000000000000000000000000000000000000
	x80, _ := decimal.NewFromString("205688069665150755269371147819668813122841983204197482918576128")
	x80Big := x80.BigInt()
	return new(big.Int).And(encodedSwap, x80Big).Int64() == 0
}
