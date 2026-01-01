import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, RepeatVector, TimeDistributed, Dense
from utils import load_data, create_sequences, FEATURES
import joblib

SYMBOL = "RELIANCE"
WINDOW = 20

df = load_data(SYMBOL)

# train ONLY on normal data
df = df[df["injected"] == False]

scaler = MinMaxScaler()
scaled = scaler.fit_transform(df[FEATURES])

X = create_sequences(scaled, WINDOW)

# ---------------- LSTM AUTOENCODER ----------------
model = Sequential([
    LSTM(64, activation="relu", input_shape=(WINDOW, len(FEATURES))),
    RepeatVector(WINDOW),
    LSTM(64, activation="relu", return_sequences=True),
    TimeDistributed(Dense(len(FEATURES)))
])

model.compile(optimizer="adam", loss="mse")
model.summary()

model.fit(
    X, X,
    epochs=20,
    batch_size=32,
    validation_split=0.1,
    verbose=1
)

model.save("lstm_model.h5")
joblib.dump(scaler, "scaler.save")

print("✅ LSTM model trained & saved")
