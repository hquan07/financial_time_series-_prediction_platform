# Phase 5 modeling method

Phase 5 compares fixed research candidates on the explicitly selected
`training_next_session_return_v1` Gold version. The contract is
`price_only_return_models_v1`, with 27 price, technical, range and volume
predictors plus train-fitted ticker and exchange encoding. The target is
`adj_open_to_close_v1`, the next exchange session's open-to-close return.

The pinned historical dataset has 4,760 rows for seven equities. Its market
retrieval time does not establish historical point-in-time availability.
Macro and full-feature readiness are false. Results therefore describe a
price-only retrospective research snapshot and cannot establish tradable
performance.

The pinned holdout was inspected while validating the Phase 5 implementation.
Treat its recorded metrics as descriptive. A later, previously unseen period
is required for a fresh confirmatory estimate.

The last 20% of common `as_of_session` dates are a final holdout. Three
consecutive 63-session validation blocks at the end of development form
expanding walk-forward folds. All assets share each boundary. Candidate
preprocessing and fitting see only the fold's training rows. A candidate is
selected by pooled out-of-fold RMSE, then MAE, then the fixed candidate order.
The selected regression candidate and two reference baselines are refit on
development and evaluated in one holdout pass. Classification is a secondary
direction study; its two fixed candidates are evaluated in the same final pass.

The regression comparison includes zero return, historical mean, ordinary
least squares, fixed XGBoost and fixed LightGBM. Direction comparison includes
the train-fold majority class and logistic regression. Model parameters, seed,
thread count, feature list and split settings are in
[`config/model_config.yaml`](../config/model_config.yaml). Version-pinned
Gold lineage, exact folds, per-fold and per-ticker metrics, support counts,
predictions and dependency versions are written to ignored
`artifacts/experiments/{experiment_id}/`. The repository document records the
method only; experiment metrics remain in local artifacts.

Run from the repository root with healthy project PostgreSQL and MinIO:

```bash
make phase5-test
make phase5-run GOLD_TRAINING_VERSION=<pinned-version> MODEL_CONFIG=config/model_config.yaml
make phase5-report EXPERIMENT_ID=<reported-id>
```

Phase 5 does not promote or serve a model or produce a backtest.
