<template>
  <!-- .table-wrapper 是 TablePageLayout 滚动链的挂载点：外层 .table-scroll-container
       负责卡片外观并 overflow-hidden，本层接收 overflow-y-auto 才能在内容超高时滚动。 -->
  <div class="table-wrapper">
    <table
      data-testid="desktop-channels"
      class="!hidden w-full table-fixed border-collapse text-sm lg:!table"
    >
      <thead>
        <tr class="border-b border-gray-100 bg-gray-50/50 text-xs font-medium uppercase tracking-wide text-gray-500 dark:border-dark-700 dark:bg-dark-800/50 dark:text-gray-400">
          <th class="w-[180px] px-4 py-3 text-center">{{ columns.name }}</th>
          <th class="w-[200px] px-4 py-3 text-left">{{ columns.description }}</th>
          <th class="w-[140px] px-4 py-3 text-left">{{ columns.platform }}</th>
          <th class="w-[380px] px-4 py-3 text-left">{{ columns.groups }}</th>
          <th class="w-[180px] px-4 py-3 text-center">{{ columns.officialSavings }}</th>
          <th class="px-4 py-3 text-left">{{ columns.supportedModels }}</th>
        </tr>
      </thead>
      <tbody v-if="loading">
        <tr>
          <td colspan="6" class="py-10 text-center">
            <Icon name="refresh" size="lg" class="inline-block animate-spin text-gray-400" />
          </td>
        </tr>
      </tbody>
      <tbody v-else-if="rows.length === 0">
        <tr>
          <td colspan="6" class="py-12 text-center">
            <Icon name="inbox" size="xl" class="mx-auto mb-3 h-12 w-12 text-gray-400" />
            <p class="text-sm text-gray-500 dark:text-gray-400">{{ emptyLabel }}</p>
          </td>
        </tr>
      </tbody>
      <!-- 每个渠道一个 tbody：首行 td rowspan 渠道名，后续行只渲染其余三列。
           tbody 之间强分隔线表达"渠道边界"，tbody 内部用淡分隔线区分平台。 -->
      <tbody
        v-else
        v-for="(channel, chIdx) in rows"
        :key="`${channel.name}-${chIdx}`"
        class="border-b-2 border-gray-200 last:border-b-0 dark:border-dark-600"
      >
        <tr
          v-for="(section, secIdx) in channel.platforms"
          :key="`${channel.name}-${section.platform}`"
          class="transition-colors hover:bg-gray-50/40 dark:hover:bg-dark-800/40"
          :class="{ 'border-t border-gray-100/70 dark:border-dark-700/50': secIdx > 0 }"
        >
          <!-- 渠道名：只在第一行渲染并用 rowspan 纵向合并 -->
          <td
            v-if="secIdx === 0"
            :rowspan="channel.platforms.length"
            class="px-4 py-3 text-center align-middle font-medium text-gray-900 dark:text-white"
          >
            {{ channel.name }}
          </td>

          <!-- 描述：独立一列，同样用 rowspan 纵向合并 -->
          <td
            v-if="secIdx === 0"
            :rowspan="channel.platforms.length"
            class="px-4 py-3 align-middle text-xs text-gray-500 dark:text-gray-400"
          >
            <template v-if="channel.description">{{ channel.description }}</template>
            <span v-else class="text-gray-400">-</span>
          </td>

          <!-- 平台徽章 -->
          <td class="align-top px-4 py-3">
            <span
              :class="[
                'inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[11px] font-medium uppercase',
                platformBadgeClass(section.platform),
              ]"
            >
              <PlatformIcon :platform="section.platform as GroupPlatform" size="xs" />
              {{ section.platform }}
            </span>
          </td>

          <!-- 分组：专属分组在前（紫色 shield 行），公开分组在后（灰色 globe 行）。 -->
          <td class="align-top px-4 py-3">
            <div class="flex flex-col gap-1.5">
              <div
                v-if="exclusiveGroups(section).length > 0"
                class="flex flex-wrap items-center gap-1.5"
              >
                <span
                  class="inline-flex items-center gap-0.5 text-[10px] font-medium uppercase text-purple-600 dark:text-purple-400"
                  :title="t('availableChannels.exclusiveTooltip')"
                >
                  <Icon name="shield" size="xs" class="h-3 w-3" />
                  {{ t('availableChannels.exclusive') }}
                </span>
                <div
                  v-for="g in exclusiveGroups(section)"
                  :key="`ex-${g.id}`"
                  class="inline-flex flex-wrap items-center gap-1"
                >
                  <GroupBadge
                    :name="g.name"
                    :platform="g.platform as GroupPlatform"
                    :subscription-type="(g.subscription_type || 'standard') as SubscriptionType"
                    :rate-multiplier="g.rate_multiplier"
                    :user-rate-multiplier="userGroupRates[g.id] ?? null"
                    always-show-rate
                  />
                  <span
                    v-if="hasPeakRate(g)"
                    class="inline-flex items-center gap-1 rounded-md bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:bg-amber-900/20 dark:text-amber-300"
                    :title="peakRateTitle(g)"
                  >
                    <Icon name="clock" size="xs" class="h-3 w-3" />
                    {{ peakRateLabel(g) }}
                  </span>
                </div>
              </div>
              <div
                v-if="publicGroups(section).length > 0"
                class="flex flex-wrap items-center gap-1.5"
              >
                <span
                  class="inline-flex items-center gap-0.5 text-[10px] font-medium uppercase text-gray-500 dark:text-gray-400"
                  :title="t('availableChannels.publicTooltip')"
                >
                  <Icon name="globe" size="xs" class="h-3 w-3" />
                  {{ t('availableChannels.public') }}
                </span>
                <div
                  v-for="g in publicGroups(section)"
                  :key="`pub-${g.id}`"
                  class="inline-flex flex-wrap items-center gap-1"
                >
                  <GroupBadge
                    :name="g.name"
                    :platform="g.platform as GroupPlatform"
                    :subscription-type="(g.subscription_type || 'standard') as SubscriptionType"
                    :rate-multiplier="g.rate_multiplier"
                    :user-rate-multiplier="userGroupRates[g.id] ?? null"
                    always-show-rate
                  />
                  <span
                    v-if="hasPeakRate(g)"
                    class="inline-flex items-center gap-1 rounded-md bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:bg-amber-900/20 dark:text-amber-300"
                    :title="peakRateTitle(g)"
                  >
                    <Icon name="clock" size="xs" class="h-3 w-3" />
                    {{ peakRateLabel(g) }}
                  </span>
                </div>
              </div>
              <span v-if="section.groups.length === 0" class="text-xs text-gray-400">-</span>
            </div>
          </td>

          <!-- 对标官方 API：官方美元价先按 USD/CNY 汇率折算，再与本站人民币倍率价比较。 -->
          <td class="px-4 py-3 text-center align-middle">
            <div
              v-if="officialSavingsPercent(section) !== null"
              class="official-savings"
              :class="{ 'official-savings--special': isSpecialPricing(section) }"
              :title="officialSavingsTitle(section)"
              :aria-label="officialSavingsAriaLabel(section)"
            >
              <span class="official-savings__label">
                {{ savingsLabel(section) }}
              </span>
              <strong
                v-if="!isSpecialPricing(section)"
                class="official-savings__value"
              >
                {{ formattedOfficialSavings(section) }}%
              </strong>
              <strong v-else class="official-savings__special">
                {{ t('availableChannels.savings.specialPricing') }}
              </strong>
            </div>
            <span v-else class="text-xs text-gray-400">-</span>
          </td>

          <!-- 支持模型 -->
          <td class="align-top px-4 py-3">
            <div class="flex flex-wrap gap-1">
              <SupportedModelChip
                v-for="m in section.supported_models"
                :key="`${section.platform}-${m.name}`"
                :model="m"
                :pricing-key-prefix="pricingKeyPrefix"
                :no-pricing-label="noPricingLabel"
                :show-platform="false"
                :platform-hint="section.platform"
                :price-multiplier="effectivePriceMultiplier(section)"
              />
              <span v-if="section.supported_models.length === 0" class="text-xs text-gray-400">
                {{ noModelsLabel }}
              </span>
            </div>
          </td>
        </tr>
      </tbody>
    </table>

    <div data-testid="mobile-channels" class="w-full min-w-0 overflow-x-hidden lg:hidden">
      <div v-if="loading" data-testid="mobile-loading" class="py-10 text-center">
        <Icon name="refresh" size="lg" class="inline-block animate-spin text-gray-400" />
      </div>
      <div v-else-if="rows.length === 0" data-testid="mobile-empty" class="py-12 text-center">
        <Icon name="inbox" size="xl" class="mx-auto mb-3 h-12 w-12 text-gray-400" />
        <p class="text-sm text-gray-500 dark:text-gray-400">{{ emptyLabel }}</p>
      </div>
      <section
        v-else
        v-for="(channel, chIdx) in rows"
        :key="`mobile-${channel.name}-${chIdx}`"
        class="border-b-2 border-gray-200 px-4 py-4 last:border-b-0 dark:border-dark-600"
      >
        <header class="mb-3 min-w-0">
          <h3 class="break-words text-sm font-semibold text-gray-900 dark:text-white">
            {{ channel.name }}
          </h3>
          <p class="mt-1 break-words text-xs leading-5 text-gray-500 dark:text-gray-400">
            {{ channel.description || '-' }}
          </p>
        </header>

        <div class="divide-y divide-gray-100 dark:divide-dark-700/60">
          <div
            v-for="section in channel.platforms"
            :key="`mobile-${channel.name}-${section.platform}`"
            class="min-w-0 py-3 first:pt-0 last:pb-0"
          >
            <span
              :class="[
                'inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[11px] font-medium uppercase',
                platformBadgeClass(section.platform),
              ]"
            >
              <PlatformIcon :platform="section.platform as GroupPlatform" size="xs" />
              {{ section.platform }}
            </span>

            <dl class="mt-3 space-y-3">
              <div class="min-w-0">
                <dt class="mb-1.5 text-[11px] font-medium text-gray-500 dark:text-gray-400">
                  {{ columns.groups }}
                </dt>
                <dd class="flex min-w-0 flex-col gap-2">
                  <div
                    v-if="exclusiveGroups(section).length > 0"
                    class="flex min-w-0 flex-wrap items-center gap-1.5"
                  >
                    <span
                      class="inline-flex items-center gap-0.5 text-[10px] font-medium uppercase text-purple-600 dark:text-purple-400"
                      :title="t('availableChannels.exclusiveTooltip')"
                    >
                      <Icon name="shield" size="xs" class="h-3 w-3" />
                      {{ t('availableChannels.exclusive') }}
                    </span>
                    <div
                      v-for="g in exclusiveGroups(section)"
                      :key="`mobile-ex-${g.id}`"
                      class="inline-flex max-w-full min-w-0 flex-wrap items-center gap-1"
                    >
                      <GroupBadge
                        class="max-w-full"
                        :name="g.name"
                        :platform="g.platform as GroupPlatform"
                        :subscription-type="(g.subscription_type || 'standard') as SubscriptionType"
                        :rate-multiplier="g.rate_multiplier"
                        :user-rate-multiplier="userGroupRates[g.id] ?? null"
                        always-show-rate
                      />
                      <span
                        v-if="hasPeakRate(g)"
                        class="inline-flex items-center gap-1 rounded-md bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:bg-amber-900/20 dark:text-amber-300"
                        :title="peakRateTitle(g)"
                      >
                        <Icon name="clock" size="xs" class="h-3 w-3" />
                        {{ peakRateLabel(g) }}
                      </span>
                    </div>
                  </div>
                  <div
                    v-if="publicGroups(section).length > 0"
                    class="flex min-w-0 flex-wrap items-center gap-1.5"
                  >
                    <span
                      class="inline-flex items-center gap-0.5 text-[10px] font-medium uppercase text-gray-500 dark:text-gray-400"
                      :title="t('availableChannels.publicTooltip')"
                    >
                      <Icon name="globe" size="xs" class="h-3 w-3" />
                      {{ t('availableChannels.public') }}
                    </span>
                    <div
                      v-for="g in publicGroups(section)"
                      :key="`mobile-pub-${g.id}`"
                      class="inline-flex max-w-full min-w-0 flex-wrap items-center gap-1"
                    >
                      <GroupBadge
                        class="max-w-full"
                        :name="g.name"
                        :platform="g.platform as GroupPlatform"
                        :subscription-type="(g.subscription_type || 'standard') as SubscriptionType"
                        :rate-multiplier="g.rate_multiplier"
                        :user-rate-multiplier="userGroupRates[g.id] ?? null"
                        always-show-rate
                      />
                      <span
                        v-if="hasPeakRate(g)"
                        class="inline-flex items-center gap-1 rounded-md bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:bg-amber-900/20 dark:text-amber-300"
                        :title="peakRateTitle(g)"
                      >
                        <Icon name="clock" size="xs" class="h-3 w-3" />
                        {{ peakRateLabel(g) }}
                      </span>
                    </div>
                  </div>
                  <span v-if="section.groups.length === 0" class="text-xs text-gray-400">-</span>
                </dd>
              </div>

              <div class="min-w-0">
                <dt class="mb-1.5 text-[11px] font-medium text-gray-500 dark:text-gray-400">
                  {{ columns.supportedModels }}
                </dt>
                <dd class="flex min-w-0 flex-wrap gap-1">
                  <SupportedModelChip
                    v-for="m in section.supported_models"
                    :key="`mobile-${section.platform}-${m.name}`"
                    class="max-w-full [&>span]:max-w-full [&>span]:truncate"
                    :model="m"
                    :pricing-key-prefix="pricingKeyPrefix"
                    :no-pricing-label="noPricingLabel"
                    :show-platform="false"
                    :platform-hint="section.platform"
                  />
                  <span v-if="section.supported_models.length === 0" class="text-xs text-gray-400">
                    {{ noModelsLabel }}
                  </span>
                </dd>
              </div>
            </dl>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import Icon from '@/components/icons/Icon.vue'
import PlatformIcon from '@/components/common/PlatformIcon.vue'
import GroupBadge from '@/components/common/GroupBadge.vue'
import SupportedModelChip from './SupportedModelChip.vue'
import type { UserAvailableChannel, UserAvailableGroup, UserChannelPlatformSection } from '@/api/channels'
import type { GroupPlatform, SubscriptionType } from '@/types'
import { platformBadgeClass } from '@/utils/platformColors'
import { useAppStore } from '@/stores/app'
import { hasPeakRate as groupHasPeakRate, formatPeakRateWindow, serverTimezoneLabel } from '@/utils/peak-rate'

const props = defineProps<{
  columns: {
    name: string
    description: string
    platform: string
    groups: string
    officialSavings: string
    supportedModels: string
  }
  rows: UserAvailableChannel[]
  loading: boolean
  pricingKeyPrefix: string
  noPricingLabel: string
  noModelsLabel: string
  emptyLabel: string
  /** 用户专属倍率（group_id → multiplier）；无专属时由 GroupBadge 仅显示默认倍率。 */
  userGroupRates: Record<number, number>
}>()

// Suppress unused warning — props is accessed via template automatically but
// the explicit reference here keeps the linter from flagging userGroupRates.
void props.userGroupRates

const { t } = useI18n()

/**
 * 官方 API 以美元计价，本站按“同数值人民币 × 分组倍率”计价。
 * 例如官方 $1 = ¥6.8，0.2x 分组实收 ¥0.2，节省 1 - 0.2/6.8 ≈ 97.1%。
 */
const OFFICIAL_USD_CNY_RATE = 6.8

function exclusiveGroups(section: UserChannelPlatformSection): UserAvailableGroup[] {
  return section.groups.filter((g) => g.is_exclusive)
}

function publicGroups(section: UserChannelPlatformSection): UserAvailableGroup[] {
  return section.groups.filter((g) => !g.is_exclusive)
}

/**
 * 线上渠道按分组拆分，因此可直接展示该组最终实收价。
 * 若将来人工把多个不同倍率分组合并到同一渠道，则保守回退基础价。
 */
function effectivePriceMultiplier(section: UserChannelPlatformSection): number {
  if (section.groups.length !== 1) return 1
  const group = section.groups[0]
  return props.userGroupRates[group.id] ?? group.rate_multiplier ?? 1
}

function effectiveGroupRates(section: UserChannelPlatformSection): number[] {
  return section.groups
    .map((group) => props.userGroupRates[group.id] ?? group.rate_multiplier ?? 1)
    .filter((rate) => Number.isFinite(rate) && rate >= 0)
}

/** 多分组渠道展示用户可获得的最大优惠；目前线上渠道均为一渠道一分组。 */
function officialSavingsPercent(section: UserChannelPlatformSection): number | null {
  const rates = effectiveGroupRates(section)
  if (rates.length === 0) return null
  const bestRate = Math.min(...rates)
  return (1 - bestRate / OFFICIAL_USD_CNY_RATE) * 100
}

function formatSavingsPercent(value: number): string {
  const rounded = Math.round(value * 10) / 10
  return Number.isInteger(rounded) ? rounded.toFixed(0) : rounded.toFixed(1)
}

function isSpecialPricing(section: UserChannelPlatformSection): boolean {
  const savings = officialSavingsPercent(section)
  return savings !== null && savings < 0
}

function formattedOfficialSavings(section: UserChannelPlatformSection): string {
  const savings = officialSavingsPercent(section)
  return savings === null ? '-' : formatSavingsPercent(savings)
}

function savingsLabel(section: UserChannelPlatformSection): string {
  if (isSpecialPricing(section)) return t('availableChannels.savings.comparedWithOfficial')
  return section.groups.length > 1
    ? t('availableChannels.savings.upTo')
    : t('availableChannels.savings.lessThanOfficial')
}

function officialSavingsTitle(section: UserChannelPlatformSection): string {
  const rates = effectiveGroupRates(section)
  const bestRate = rates.length > 0 ? Math.min(...rates) : 1
  return t('availableChannels.savings.tooltip', {
    rate: OFFICIAL_USD_CNY_RATE,
    multiplier: bestRate,
  })
}

function officialSavingsAriaLabel(section: UserChannelPlatformSection): string {
  const savings = officialSavingsPercent(section)
  if (savings === null || savings < 0) return t('availableChannels.savings.specialPricing')
  return t('availableChannels.savings.ariaLabel', {
    percent: formatSavingsPercent(savings),
    rate: OFFICIAL_USD_CNY_RATE,
  })
}

const appStore = useAppStore()

function hasPeakRate(group: UserAvailableGroup): boolean {
  return groupHasPeakRate(group)
}

function peakRateLabel(group: UserAvailableGroup): string {
  return formatPeakRateWindow(group, serverTimezoneLabel(appStore.cachedPublicSettings?.server_utc_offset))
}

function peakRateTitle(group: UserAvailableGroup): string {
  return t('common.peakRateTooltip', { window: peakRateLabel(group) }) + t('common.peakRateImageNote')
}
</script>

<style scoped>
.official-savings {
  position: relative;
  isolation: isolate;
  display: inline-flex;
  min-width: 142px;
  flex-direction: column;
  align-items: center;
  overflow: hidden;
  border: 1px solid rgb(244 114 182 / 22%);
  border-radius: 0.75rem;
  padding: 0.35rem 0.7rem 0.4rem;
  background:
    radial-gradient(circle at 12% 20%, rgb(251 146 60 / 14%), transparent 42%),
    radial-gradient(circle at 88% 78%, rgb(34 211 238 / 12%), transparent 46%),
    rgb(255 255 255 / 82%);
  box-shadow: inset 0 1px 0 rgb(255 255 255 / 75%), 0 3px 14px rgb(236 72 153 / 7%);
}

.official-savings::after {
  position: absolute;
  z-index: -1;
  top: -80%;
  left: -35%;
  width: 30%;
  height: 260%;
  content: '';
  transform: rotate(18deg);
  background: linear-gradient(90deg, transparent, rgb(255 255 255 / 72%), transparent);
  animation: official-savings-sheen 5.5s ease-in-out infinite;
}

.official-savings__label {
  color: rgb(107 114 128);
  font-size: 9px;
  font-weight: 650;
  letter-spacing: 0.1em;
  line-height: 1.1;
  text-transform: uppercase;
}

.official-savings__value {
  margin-top: 0.08rem;
  background: linear-gradient(92deg, #f97316 2%, #ec4899 30%, #8b5cf6 56%, #06b6d4 80%, #10b981 98%);
  background-size: 220% auto;
  background-clip: text;
  color: transparent;
  font-size: 1.15rem;
  font-weight: 850;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.03em;
  line-height: 1.15;
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  animation: official-savings-gradient 4s linear infinite;
}

.official-savings--special {
  border-color: rgb(245 158 11 / 24%);
  background: rgb(255 251 235 / 82%);
}

.official-savings__special {
  margin-top: 0.2rem;
  color: rgb(180 83 9);
  font-size: 0.75rem;
}

:global(.dark) .official-savings {
  border-color: rgb(244 114 182 / 20%);
  background:
    radial-gradient(circle at 12% 20%, rgb(249 115 22 / 14%), transparent 42%),
    radial-gradient(circle at 88% 78%, rgb(6 182 212 / 12%), transparent 46%),
    rgb(17 24 39 / 72%);
  box-shadow: inset 0 1px 0 rgb(255 255 255 / 5%), 0 4px 16px rgb(0 0 0 / 12%);
}

:global(.dark) .official-savings__label {
  color: rgb(156 163 175);
}

@keyframes official-savings-gradient {
  to { background-position: 220% center; }
}

@keyframes official-savings-sheen {
  0%, 64% { transform: translateX(-180%) rotate(18deg); opacity: 0; }
  70% { opacity: 0.8; }
  86%, 100% { transform: translateX(620%) rotate(18deg); opacity: 0; }
}

@media (prefers-reduced-motion: reduce) {
  .official-savings::after,
  .official-savings__value {
    animation: none;
  }
}
</style>
