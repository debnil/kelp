package plugins

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/openlyinc/pointy"
	"github.com/stellar/kelp/queries"
	"github.com/stellar/kelp/support/utils"

	"github.com/stellar/go/txnbuild"

	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/kelp/model"
	"github.com/stretchr/testify/assert"
)

func makeWantVolumeFilter(config *VolumeFilterConfig, marketIDs []string, accountIDs []string, action string) *volumeFilter {
	query, e := queries.MakeDailyVolumeByDateForMarketIdsAction(&sql.DB{}, marketIDs, action, accountIDs)
	if e != nil {
		panic(e)
	}

	return &volumeFilter{
		name:                   "volumeFilter",
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
			// this lets us run the for-loop below for both base and quote units within the config
			baseCapInBaseConfig := makeRawVolumeFilterConfig(
				pointy.Float64(1.0),
				nil,
				m,
				k.marketIDs,
				k.accountIDs,
			)
			baseCapInQuoteConfig := makeRawVolumeFilterConfig(
				nil,
				pointy.Float64(1.0),
				m,
				k.marketIDs,
				k.accountIDs,
			)
			for _, config := range []*VolumeFilterConfig{baseCapInBaseConfig, baseCapInQuoteConfig} {
				// configType is used to represent the type of config when printing test name
				configType := "quote"
				if config.SellBaseAssetCapInBaseUnits != nil {
					configType = "base"
				}

				// TODO DS Vary filter action between buy and sell, once buy logic is implemented.
				wantFilter := makeWantVolumeFilter(config, k.wantMarketIDs, k.accountIDs, "sell")
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

func makeManageSellOffer(price, amount string) *txnbuild.ManageSellOffer {
	if amount == "" {
		return nil
	}

	return &txnbuild.ManageSellOffer{
		Buying:  txnbuild.NativeAsset{},
		Selling: txnbuild.NativeAsset{},
		Price:   price,
		Amount:  amount,
	}
}

func TestVolumeFilterFn(t *testing.T) {
	testCases := []struct {
		name               string
		filter             *volumeFilter
		sellBaseCapInBase  *float64
		sellBaseCapInQuote *float64
		otbBaseCap         float64
		otbQuoteCap        float64
		tbbBaseCap         float64
		tbbQuoteCap        float64
		price              string
		inputAmount        string
		wantAmount         string
		wantTbbBaseCap     float64
		wantTbbQuoteCap    float64
	}{
		{
			name:               "selling, base units sell cap, don't keep selling base",
			sellBaseCapInBase:  pointy.Float64(0.0),
			sellBaseCapInQuote: nil,
			otbBaseCap:         0.0,
			otbQuoteCap:        0.0,
			tbbBaseCap:         0.0,
			tbbQuoteCap:        0.0,
			price:              "2.0",
			inputAmount:        "100.0",
			wantAmount:         "",
			wantTbbBaseCap:     0.0,
			wantTbbQuoteCap:    0.0,
		},
		{
			name:               "selling, base units sell cap, keep selling base",
			sellBaseCapInBase:  pointy.Float64(1.0),
			sellBaseCapInQuote: nil,
			otbBaseCap:         0.0,
			otbQuoteCap:        0.0,
			tbbBaseCap:         0.0,
			tbbQuoteCap:        0.0,
			price:              "2.0",
			inputAmount:        "100.0",
			wantAmount:         "1.0000000",
			wantTbbBaseCap:     1.0,
			wantTbbQuoteCap:    2.0,
		},
		{
			name:               "selling, quote units sell cap, don't keep selling quote",
			sellBaseCapInBase:  nil,
			sellBaseCapInQuote: pointy.Float64(0),
			otbBaseCap:         0.0,
			otbQuoteCap:        0.0,
			tbbBaseCap:         0.0,
			tbbQuoteCap:        0.0,
			price:              "2.0",
			inputAmount:        "100.0",
			wantAmount:         "",
			wantTbbBaseCap:     0.0,
			wantTbbQuoteCap:    0.0,
		},
		{
			name:               "selling, quote units sell cap, keep selling quote",
			sellBaseCapInBase:  nil,
			sellBaseCapInQuote: pointy.Float64(1.),
			otbBaseCap:         0.0,
			otbQuoteCap:        0.0,
			tbbBaseCap:         0.0,
			tbbQuoteCap:        0.0,
			price:              "2.0",
			inputAmount:        "100.0",
			wantAmount:         "0.5000000",
			wantTbbBaseCap:     0.5,
			wantTbbQuoteCap:    1.0,
		},
		{
			name:               "selling, base and quote units sell cap, keep selling base and quote",
			sellBaseCapInBase:  pointy.Float64(1.),
			sellBaseCapInQuote: pointy.Float64(1.),
			otbBaseCap:         0.0,
			otbQuoteCap:        0.0,
			tbbBaseCap:         0.0,
			tbbQuoteCap:        0.0,
			price:              "2.0",
			inputAmount:        "100.0",
			wantAmount:         "0.5000000",
			wantTbbBaseCap:     0.5,
			wantTbbQuoteCap:    1.0,
		},
	}

	for _, k := range testCases {
		t.Run(k.name, func(t *testing.T) {
			marketIDs := []string{}
			accountIDs := []string{}
			mode := volumeFilterModeExact
			dailyOTB := makeTestVolumeFilterConfig(k.otbBaseCap, k.otbQuoteCap, marketIDs, accountIDs, mode)
			dailyTBB := makeTestVolumeFilterConfig(k.tbbBaseCap, k.tbbQuoteCap, marketIDs, accountIDs, mode)
			wantTBB := makeTestVolumeFilterConfig(k.wantTbbBaseCap, k.wantTbbQuoteCap, marketIDs, accountIDs, mode)
			op := makeManageSellOffer(k.price, k.inputAmount)
			wantOp := makeManageSellOffer(k.price, k.wantAmount)

			lp := limitParameters{
				sellBaseAssetCapInBaseUnits:  k.sellBaseCapInBase,
				sellBaseAssetCapInQuoteUnits: k.sellBaseCapInQuote,
				mode:                         volumeFilterModeExact,
			}

			actual, e := volumeFilterFn(dailyOTB, dailyTBB, op, utils.NativeAsset, utils.NativeAsset, lp)

			assert.Nil(t, e)
			assert.Equal(t, wantOp, actual)
			assert.Equal(t, wantTBB, dailyTBB)
		})
	}
}
