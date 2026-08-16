package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/ent/paymentorder"
	"github.com/Wei-Shaw/sub2api/internal/payment"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUserRepositoryProfileUpdateDoesNotOverwriteCurrentBillingCurrency(t *testing.T) {
	repo, client := newUserEntRepo(t)
	ctx := context.Background()
	user := &service.User{
		Email: "stale-currency@example.com", Username: "before", PasswordHash: "hash",
		Role: service.RoleUser, Status: service.StatusActive, BillingCurrency: service.CurrencyCNY,
	}
	require.NoError(t, repo.Create(ctx, user))
	stale, err := repo.GetByID(ctx, user.ID)
	require.NoError(t, err)
	require.NoError(t, client.User.UpdateOneID(user.ID).SetBillingCurrency(service.CurrencyUSD).Exec(ctx))

	stale.Username = "after"
	require.NoError(t, repo.Update(ctx, stale))
	reloaded, err := client.User.Get(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, "after", reloaded.Username)
	require.Equal(t, service.CurrencyUSD, reloaded.BillingCurrency)
}

func TestUserRepositoryExplicitCurrencyChangeRechecksLockedBalance(t *testing.T) {
	repo, client := newUserEntRepo(t)
	ctx := context.Background()
	user := &service.User{
		Email: "currency-balance-guard@example.com", Username: "guard", PasswordHash: "hash",
		Role: service.RoleUser, Status: service.StatusActive, BillingCurrency: service.CurrencyCNY,
	}
	require.NoError(t, repo.Create(ctx, user))
	stale, err := repo.GetByID(ctx, user.ID)
	require.NoError(t, err)
	require.NoError(t, client.User.UpdateOneID(user.ID).AddBalance(5).Exec(ctx))
	stale.BillingCurrency = service.CurrencyUSD

	err = repo.Update(service.ContextWithBillingCurrencyUpdate(ctx), stale)
	require.Equal(t, "BILLING_CURRENCY_BALANCE_NOT_EMPTY", infraerrors.Reason(err))
	reloaded, getErr := client.User.Get(ctx, user.ID)
	require.NoError(t, getErr)
	require.Equal(t, service.CurrencyCNY, reloaded.BillingCurrency)
	require.Equal(t, 5.0, reloaded.Balance)
}

func TestUserRepositoryExplicitCurrencyChangeRejectsPendingBalanceOrder(t *testing.T) {
	repo, client := newUserEntRepo(t)
	ctx := context.Background()
	user := &service.User{
		Email: "currency-order-guard@example.com", Username: "guard", PasswordHash: "hash",
		Role: service.RoleUser, Status: service.StatusActive, BillingCurrency: service.CurrencyCNY,
	}
	require.NoError(t, repo.Create(ctx, user))
	_, err := client.PaymentOrder.Create().
		SetUserID(user.ID).SetUserEmail(user.Email).SetUserName(user.Username).
		SetAmount(10).SetPayAmount(10).SetFeeRate(0).SetRechargeCode("PENDING-CURRENCY-GUARD").
		SetOutTradeNo("pending-currency-guard").SetPaymentType(payment.TypeAlipay).
		SetPaymentTradeNo("").SetOrderType(payment.OrderTypeBalance).SetStatus(service.OrderStatusPending).
		SetExpiresAt(user.CreatedAt.AddDate(1, 0, 0)).SetClientIP("127.0.0.1").SetSrcHost("test").
		Save(ctx)
	require.NoError(t, err)
	stale, err := repo.GetByID(ctx, user.ID)
	require.NoError(t, err)
	stale.BillingCurrency = service.CurrencyUSD

	err = repo.Update(service.ContextWithBillingCurrencyUpdate(ctx), stale)
	require.Equal(t, "BILLING_CURRENCY_PENDING_ORDER", infraerrors.Reason(err))
	require.True(t, client.PaymentOrder.Query().Where(paymentorder.UserIDEQ(user.ID)).ExistX(ctx))
}

func TestUserRepositoryExplicitCurrencyChangeRejectsRefundInProgress(t *testing.T) {
	repo, client := newUserEntRepo(t)
	ctx := context.Background()
	user := &service.User{
		Email: "currency-refund-guard@example.com", Username: "guard", PasswordHash: "hash",
		Role: service.RoleUser, Status: service.StatusActive, BillingCurrency: service.CurrencyCNY,
	}
	require.NoError(t, repo.Create(ctx, user))
	_, err := client.PaymentOrder.Create().
		SetUserID(user.ID).SetUserEmail(user.Email).SetUserName(user.Username).
		SetAmount(10).SetPayAmount(10).SetFeeRate(0).SetRechargeCode("REFUNDING-CURRENCY-GUARD").
		SetOutTradeNo("refunding-currency-guard").SetPaymentType(payment.TypeAlipay).
		SetPaymentTradeNo("trade-refunding").SetOrderType(payment.OrderTypeBalance).SetStatus(service.OrderStatusRefunding).
		SetExpiresAt(user.CreatedAt.AddDate(1, 0, 0)).SetClientIP("127.0.0.1").SetSrcHost("test").
		Save(ctx)
	require.NoError(t, err)
	stale, err := repo.GetByID(ctx, user.ID)
	require.NoError(t, err)
	stale.BillingCurrency = service.CurrencyUSD

	err = repo.Update(service.ContextWithBillingCurrencyUpdate(ctx), stale)
	require.Equal(t, "BILLING_CURRENCY_PENDING_ORDER", infraerrors.Reason(err))
}

func TestRedeemAndPromoWalletLocksAreSQLiteCompatible(t *testing.T) {
	repo, client := newUserEntRepo(t)
	ctx := context.Background()
	user := &service.User{
		Email: "sqlite-wallet-locks@example.com", Username: "wallet", PasswordHash: "hash",
		Role: service.RoleUser, Status: service.StatusActive, BillingCurrency: service.CurrencyCNY,
	}
	require.NoError(t, repo.Create(ctx, user))

	redeemRepo := NewRedeemCodeRepository(client)
	redeemSvc := service.NewRedeemService(redeemRepo, repo, nil, nil, nil, client, nil, nil)
	require.NoError(t, redeemSvc.CreateCode(ctx, &service.RedeemCode{
		Code: "SQLITE-REDEEM-CNY", Type: service.RedeemTypeBalance, Value: 3,
		Currency: service.CurrencyCNY, Status: service.StatusUnused,
	}))
	_, err := redeemSvc.Redeem(ctx, user.ID, "SQLITE-REDEEM-CNY")
	require.NoError(t, err)

	promoRepo := NewPromoCodeRepository(client)
	promoSvc := service.NewPromoService(promoRepo, repo, nil, client, nil)
	require.NoError(t, promoRepo.Create(ctx, &service.PromoCode{
		Code: "SQLITE-PROMO-CNY", BonusAmount: 2, Currency: service.CurrencyCNY,
		MaxUses: 1, Status: service.PromoCodeStatusActive,
	}))
	require.NoError(t, promoSvc.ApplyPromoCode(ctx, user.ID, "SQLITE-PROMO-CNY"))

	reloaded, err := repo.GetByID(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, 5.0, reloaded.Balance)
}
