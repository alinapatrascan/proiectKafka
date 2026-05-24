# split_dataset.py — rulat o singura data pentru a genera cele 3 CSV-uri
import pandas as pd

df = pd.read_csv("fast_food_health.csv")
df.insert(0, "respondent_id", range(800))

# Membru 1 — Demographics
df[["respondent_id", "Age", "Gender"]].to_csv("data/demographics.csv", index=False)

# Membru 2 — Consumption
df[["respondent_id", "Fast_Food_Meals_Per_Week", "Average_Daily_Calories"]].to_csv("data/consumption.csv", index=False)

# Membru 3 — Health Impact
df[[
    "respondent_id",
    "BMI",
    "Physical_Activity_Hours_Per_Week",
    "Sleep_Hours_Per_Day",
    "Energy_Level_Score",
    "Digestive_Issues",
    "Doctor_Visits_Per_Year",
    "Overall_Health_Score"
]].to_csv("data/health.csv", index=False)

print("Done.")
print(pd.read_csv("data/demographics.csv").head(3))
print(pd.read_csv("data/consumption.csv").head(3))
print(pd.read_csv("data/health.csv").head(3))