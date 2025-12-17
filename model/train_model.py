import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# ---- LOAD RAW CSV (NO HEADER) ----
df = pd.read_csv(
    "data/raw_history.csv",
    header=None,
    names=["symbol", "open", "high", "low", "close", "volume", "timestamp"]
)

# ---- FIX TIMESTAMP ----
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

# ---- SORT ----
df = df.sort_values(["symbol", "timestamp"])

# ---- FEATURE ENGINEERING ----
df["return"] = df.groupby("symbol")["close"].pct_change()
df["volatility"] = df.groupby("symbol")["return"].rolling(5).std().reset_index(0, drop=True)
df["volume_change"] = df.groupby("symbol")["volume"].pct_change()

# ---- DROP WARMUP ROWS ----
df = df.dropna()

# ---- FRAUD LABEL (UNSUPERVISED HEURISTIC) ----
df["fraud"] = (
    (df["return"].abs() > 0.01) |        # >1% jump
    (df["volume_change"].abs() > 2.0)    # >200% volume spike
).astype(int)

# ---- FEATURES ----
X = df[["return", "volatility", "volume_change"]]
y = df["fraud"]

# ---- TRAIN / TEST ----
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)

# ---- MODEL ----
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1,
    eval_metric="logloss"
)

model.fit(X_train, y_train)

# ---- EVALUATE ----
print(classification_report(y_test, model.predict(X_test)))

# ---- SAVE MODEL ----
joblib.dump(model, "model/fraud_xgb.pkl")
print("✅ Model trained and saved")
