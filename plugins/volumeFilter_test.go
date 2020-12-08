package plugins

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/openlyinc/pointy"
	"github.com/stellar/kelp/queries"
	"github.com/stellar/kelp/support/utils"

	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/kelp/model"
	"github.com/stretchr/testify/assert"
)

func makeWantVolumeFilter(config *VolumeFilterConfig, marketIDs []string, accountIDs []string, action queries.DailyVolumeAction) *volumeFilter {
	query, e := queries.MakeDailyVolumeByDateForMarketIdsAction(&sql.DB{}, marketIDs, action, accountIDs)
	if e != nil {
		panic(e)
	}

	return &volumeFilter{
		name:                   "volumeFilter",
		configValue:            "",
		baseAsset:              utils.NativeAsset,
		quoteAsset:             utils.NativeAsset,
		config:                 config,
		dailyVolumeByDateQuery: query,
	}
}

func TestMakeFilterVolume(t *testing.T) {
	testAssetDisplayFn := model.MakeSdexMappedAssetDisplayFn(map[model.Asset]hProtocol.Asset{model.Asset("XLM"): utils.NativeAsset})
	configValue := ""
	tradingPair := &model.TradingPair{Base: "XLM", Quote: "XLM"}
	modes := []volumeFilterMode{volumeFilterModeExact, volumeFilterModeIgnore}

	testCases := []struct {
		name          string
		exchangeName  string
		marketIDs     []string
		accountIDs    []string
		wantMarketIDs []string
		wantFilter    *volumeFilter
	}{
		// TODO DS Confirm the empty config fails once validation is added to the constructor
		{
			name:          "0 market id or account id",
			exchangeName:  "exchange 2",
			marketIDs:     []string{},
			accountIDs:    []string{},
			wantMarketIDs: []string{"9db20cdd56"},
		},
		{
			name:          "1 market id",
			exchangeName:  "exchange 1",
			marketIDs:     []string{"marketID"},
			accountIDs:    []string{},
			wantMarketIDs: []string{"6d9862b0e2", "marketID"},
		},
		{
			name:          "2 market ids",
			exchangeName:  "exchange 2",
			marketIDs:     []string{"marketID1", "marketID2"},
			accountIDs:    []string{},
			wantMarketIDs: []string{"9db20cdd56", "marketID1", "marketID2"},
		},
		{
			name:          "2 dupe market ids, 1 distinct",
			exchangeName:  "exchange 1",
			marketIDs:     []string{"marketID1", "marketID1", "marketID2"},
			accountIDs:    []string{},
			wantMarketIDs: []string{"6d9862b0e2", "marketID1", "marketID2"},
		},
		{
			name:          "1 account id",
			exchangeName:  "exchange 2",
			marketIDs:     []string{},
			accountIDs:    []string{"accountID"},
			wantMarketIDs: []string{"9db20cdd56"},
		},
		{
			name:          "2 account ids",
			exchangeName:  "exchange 1",
			marketIDs:     []string{},
			accountIDs:    []string{"accountID1", "accountID2"},
			wantMarketIDs: []string{"6d9862b0e2"},
		},
		{
			name:          "account and market ids",
			exchangeName:  "exchange 2",
			marketIDs:     []string{"marketID"},
			accountIDs:    []string{"accountID"},
			wantMarketIDs: []string{"9db20cdd56", "marketID"},
		},
	}

	for _, k := range testCases {
		// this lets us test both types of modes when varying the market and account ids
		for _, m := range modes {
			// this lets us test both buy and sell
			// TODO DS Add buy action
			for _, action := range []queries.DailyVolumeAction{queries.DailyVolumeActionSell} {
				// this lets us run the for-loop below for both base and quote units within the config
				baseCapInBaseConfig := makeRawVolumeFilterConfig(
					pointy.Float64(1.0),
					nil,
					action,
					m,
					k.marketIDs,
					k.accountIDs,
				)
				baseCapInQuoteConfig := makeRawVolumeFilterConfig(
					nil,
					pointy.Float64(1.0),
					action,
					m,
					k.marketIDs,
					k.accountIDs,
				)
				for _, config := range []*VolumeFilterConfig{baseCapInBaseConfig, baseCapInQuoteConfig} {
					// configType is used to represent the type of config when printing test name
					configType := "quote"
					if config.BaseAssetCapInBaseUnits != nil {
						configType = "base"
					}

					// TODO DS Vary filter action between buy and sell, once buy logic is implemented.
					wantFilter := makeWantVolumeFilter(config, k.wantMarketIDs, k.accountIDs, action)
					t.Run(fmt.Sprintf("%s/%s/%s", k.name, configType, m), func(t *testing.T) {
						actual, e := makeFilterVolume(
							configValue,
							k.exchangeName,
							tradingPair,
							testAssetDisplayFn,
							utils.NativeAsset,
							utils.NativeAsset,
							&sql.DB{},
							config,
						)

						if !assert.Nil(t, e) {
							return
						}

						assert.Equal(t, wantFilter, actual)
					})
				}
			}
		}
	}
}

func TestVolumeFilterFn(t *testing.T) {
	testCases := []struct {
		name           string
		mode           volumeFilterMode
		baseCapInBase  *float64
		baseCapInQuote *float64
		otbBase        *float64
		otbQuote       *float64
		tbbBase        *float64
		tbbQuote       *float64
		inputOp        *txnbuild.ManageSellOffer
		wantOp         *txnbuild.ManageSellOffer
		wantTbbBase    *float64
		wantTbbQuote   *float64
	}{
		// These are all the paths that can happen, assuming exactly one quote exists (a check before this function).
		// Note that when there is no base cap, projected sold = otb base + tbb base + op amount
		// no quote cap; base cap, projected sold < cap -> keep selling base with same amount
		{
			name:           "1. selling, only base cap, projected sold < cap",
			mode:           volumeFilterModeExact,
			baseCapInBase:  pointy.Float64(10.0),
			baseCapInQuote: nil,
			otbBase:        pointy.Float64(2.5),
			otbQuote:       pointy.Float64(0.0),
			tbbBase:        pointy.Float64(5.5),
			tbbQuote:       pointy.Float64(0.0),
			inputOp:        makeManageSellOffer("2.0", "1.99"),
			wantOp:         makeManageSellOffer("2.0", "1.99"),
			wantTbbBase:    pointy.Float64(7.49),
			wantTbbQuote:   pointy.Float64(3.98),
		},
		// no quote cap; base cap, projected sold = cap -> keep selling base with same amount
		{
			name:           "2. selling, only base cap, projected sold = cap",
			mode:           volumeFilterModeExact,
			baseCapInBase:  pointy.Float64(10.0),
			baseCapInQuote: nil,
			otbBase:        pointy.Float64(2.5),
			otbQuote:       pointy.Float64(0.0),
			tbbBase:        pointy.Float64(5.5),
			tbbQuote:       pointy.Float64(0.0),
			inputOp:        makeManageSellOffer("2.0", "2.0"),
			wantOp:         makeManageSellOffer("2.0", "2.0"),
			wantTbbBase:    pointy.Float64(7.5),
			wantTbbQuote:   pointy.Float64(4.0),
		},
		// no quote cap; base cap, projected sold > cap, exact, no capacity -> don't keep selling base
		{
			name:           "3. selling, only base cap, projected sold > cap, exact, no capacity",
			mode:           volumeFilterModeExact,
			baseCapInBase:  pointy.Float64(5.0),
			baseCapInQuote: nil,
			otbBase:        pointy.Float64(2.5),
			otbQuote:       pointy.Float64(0.0),
			tbbBase:        pointy.Float64(2.5),
			tbbQuote:       pointy.Float64(0.0),
			inputOp:        makeManageSellOffer("2.0", "6.00"),
			wantOp:         nil,
			wantTbbBase:    pointy.Float64(2.5),
			wantTbbQuote:   pointy.Float64(0.0),
		},
		// no quote cap; base cap, projected sold > cap, ignore -> nil
		{
			name:           "4. selling, only base cap, projected sold > cap, ignore",
			mode:           volumeFilterModeIgnore,
			baseCapInBase:  pointy.Float64(10.0),
			baseCapInQuote: nil,
			otbBase:        pointy.Float64(2.5),
			otbQuote:       pointy.Float64(0.0),
			tbbBase:        pointy.Float64(5.5),
			tbbQuote:       pointy.Float64(0.0),
			inputOp:        makeManageSellOffer("2.0", "2.01"),
			wantOp:         nil,
			wantTbbBase:    pointy.Float64(5.5),
			wantTbbQuote:   pointy.Float64(0.0),
		},
		// no quote cap; base cap, projected sold > cap, exact, yes capacity -> keep selling base with updated amount
		{
			name:           "5. selling, only base cap, projected sold > cap, exact, yes capacity",
			mode:           volumeFilterModeExact,
			baseCapInBase:  pointy.Float64(10.0),
			baseCapInQuote: nil,
			otbBase:        pointy.Float64(2.5),
			otbQuote:       pointy.Float64(0.0),
			tbbBase:        pointy.Float64(5.5),
			tbbQuote:       pointy.Float64(0.0),
			inputOp:        makeManageSellOffer("2.0", "2.01"),
			wantOp:         makeManageSellOffer("2.0", "2.0000000"),
			wantTbbBase:    pointy.Float64(7.5),
			wantTbbQuote:   pointy.Float64(4.0),
		},
		// no base cap; quote cap, projected sold < cap -> keep selling quote with same amount
		{
			name:           "6. selling, only quote cap, projected sold < cap",
			mode:           volumeFilterModeExact,
			baseCapInBase:  nil,
			baseCapInQuote: pointy.Float64(10.0),
			otbBase:        pointy.Float64(0.0),
			otbQuote:       pointy.Float64(2.5),
			tbbBase:        pointy.Float64(0.0),
			tbbQuote:       pointy.Float64(5.5),
			inputOp:        makeManageSellOffer("2.0", "0.99"),
			wantOp:         makeManageSellOffer("2.0", "0.99"),
			wantTbbBase:    pointy.Float64(0.99),
			wantTbbQuote:   pointy.Float64(7.48),
		},
		// no base cap; quote cap, projected sold = cap -> keep selling quote with same amount
		{
			name:           "7. selling, only quote cap, projected sold = cap",
			mode:           volumeFilterModeExact,
			baseCapInBase:  nil,
			baseCapInQuote: pointy.Float64(10.0),
			otbBase:        pointy.Float64(0.0),
			otbQuote:       pointy.Float64(2.5),
			tbbBase:        pointy.Float64(0.0),
			tbbQuote:       pointy.Float64(5.5),
			inputOp:        makeManageSellOffer("2.0", "1.0"),
			wantOp:         makeManageSellOffer("2.0", "1.0"),
			wantTbbBase:    pointy.Float64(1.0),
			wantTbbQuote:   pointy.Float64(7.5),
		},
		// no base cap; quote cap, projected sold > cap, not exact -> don't keep selling quote
		{
			name:           "8. selling, only quote cap, projected sold > cap, not exact",
			mode:           volumeFilterModeIgnore,
			baseCapInBase:  nil,
			baseCapInQuote: pointy.Float64(5.0),
			otbBase:        pointy.Float64(0.0),
			otbQuote:       pointy.Float64(2.5),
			tbbBase:        pointy.Float64(0.0),
			tbbQuote:       pointy.Float64(2.5),
			inputOp:        makeManageSellOffer("2.0", "3.00"),
			wantOp:         nil,
			wantTbbBase:    pointy.Float64(0.0),
			wantTbbQuote:   pointy.Float64(2.5),
		},
		// no base cap; quote cap, projected sold > cap, exact, no capacity -> don't keep selling quote
		{
			name:           "9. selling, only quote cap, projected sold > cap, exact, no capacity",
			mode:           volumeFilterModeExact,
			baseCapInBase:  nil,
			baseCapInQuote: pointy.Float64(25.),
			otbBase:        pointy.Float64(0.0),
			otbQuote:       pointy.Float64(25.0),
			tbbBase:        pointy.Float64(0.0),
			tbbQuote:       pointy.Float64(1.0),
			inputOp:        makeManageSellOffer("3.0", "10.0"),
			wantOp:         nil,
			wantTbbBase:    pointy.Float64(0.0),
			wantTbbQuote:   pointy.Float64(1.0),
		},
		// no base cap; quote cap, projected sold > cap, exact, yes capacity -> keep selling quote with updated amount
		{
			name:           "10. selling, only quote cap, projected sold > cap, exact, yes capacity",
			mode:           volumeFilterModeExact,
			baseCapInBase:  nil,
			baseCapInQuote: pointy.Float64(50.),
			otbBase:        pointy.Float64(0.0),
			otbQuote:       pointy.Float64(25.0),
			tbbBase:        pointy.Float64(0.0),
			tbbQuote:       pointy.Float64(1.0),
			inputOp:        makeManageSellOffer("3.0", "10.0"),
			wantOp:         makeManageSellOffer("3.0", "8.0000000"),
			wantTbbBase:    pointy.Float64(8.0),
			wantTbbQuote:   pointy.Float64(25.0),
		},
	}

	// we fix the marketIDs and accountIDs, since volumeFilterFn output does not depend on them
	marketIDs := []string{}
	accountIDs := []string{}

	for _, k := range testCases {
		for _, action := range []queries.DailyVolumeAction{queries.DailyVolumeActionSell} {
			t.Run(k.name, func(t *testing.T) {
				// exactly one of the two cap values must be set
				if k.baseCapInBase == nil && k.baseCapInQuote == nil {
					assert.Fail(t, "either one of the two cap values must be set")
					return
				}

				if k.baseCapInBase != nil && k.baseCapInQuote != nil {
					assert.Fail(t, "both of the cap values cannot be set")
					return
				}

				dailyOTB := makeRawVolumeFilterConfig(k.otbBase, k.otbQuote, action, k.mode, marketIDs, accountIDs)
				dailyTBBAccumulator := makeRawVolumeFilterConfig(k.tbbBase, k.tbbQuote, action, k.mode, marketIDs, accountIDs)
				lp := limitParameters{
					baseAssetCapInBaseUnits:  k.baseCapInBase,
					baseAssetCapInQuoteUnits: k.baseCapInQuote,
					mode:                     k.mode,
				}

				actual, e := volumeFilterFn(dailyOTB, dailyTBBAccumulator, k.inputOp, utils.NativeAsset, utils.NativeAsset, lp)
				if !assert.Nil(t, e) {
					return
				}
				assert.Equal(t, k.wantOp, actual)

				wantTBBAccumulator := makeRawVolumeFilterConfig(k.wantTbbBase, k.wantTbbQuote, action, k.mode, marketIDs, accountIDs)
				assert.Equal(t, wantTBBAccumulator, dailyTBBAccumulator)
			})
		}
	}
}

func makeManageSellOffer(price string, amount string) *txnbuild.ManageSellOffer {
	return &txnbuild.ManageSellOffer{
		Buying:  txnbuild.NativeAsset{},
		Selling: txnbuild.NativeAsset{},
		Price:   price,
		Amount:  amount,
	}
}
