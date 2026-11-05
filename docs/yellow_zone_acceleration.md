# Yellow-zone acceleration update

Keep the existing minimum-gap reference and n-band ratio. Remove the price/m alternative completely; retain the old m endpoint only as an ignored compatibility argument and remove it from the supplied command. Bullish requires lower net decline, upper net rise >= n*lower decline, upper latest completed 4h increment >0 and greater than its preceding increment. Bearish requires upper net rise, lower net decline >= n*upper rise, lower latest increment <0 and its absolute value greater than the preceding increment's absolute value (the user's -3 -> -4 definition). Increments are band[t]-band[t-1] and band[t-1]-band[t-2], using completed native 4h candles only. The earlier 'lower rises' wording is interpreted as declines per the explicit negative example. Missing/tied slopes do not qualify. Still range-only, no extreme overlap.

New range_transition_zones_enabled boolean default true disables all yellow-zone classification, shading, entry bans and zone exits when false; other stretch/range/cross rules and universal veto stay active. No live orders.

Plan: failing formula/causality/toggle/endpoint tests, implement; update obsolete price-branch tests, full suite/compile/shell, review, update package. Tabs; copied archive workspace, not a git checkout.

Progress: six new rule/toggle/endpoint/causality tests observed RED then GREEN; added full simulation comparison with identical candles and toggle on/off. Previous price-branch tests were updated for intentionally removed behavior. Fresh reviewer found no functional issues. Deprecated m retains prior positive finite validation. Final verification: 310 tests passed in 52.914s; Python compilation and runner shell syntax checks passed.
