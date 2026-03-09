import pandas as pd
import matplotlib.pyplot as plt
import os

def main():
    # Load raw data
    raw_path = "data/raw_scada_2017.csv"
    if not os.path.exists(raw_path):
        print(f"File {raw_path} not found. Run scripts/edp_processing.py first.")
        return

    # Inland dataset mapping
    df = pd.read_csv(raw_path)
    
    # Mapping
    wind_speed = df['V']
    power_percent = df['y (% relative to rated power)']
    
    # Create directory for images
    os.makedirs("data/insights", exist_ok=True)

    # 1. Power Curve
    plt.figure(figsize=(10, 6))
    plt.scatter(wind_speed, power_percent, alpha=0.1, color='blue')
    plt.title('Turbine Power Curve (Inland Wind Farm WT1)')
    plt.xlabel('Wind Speed (m/s)')
    plt.ylabel('Power Output (% of Rated)')
    plt.grid(True)
    plt.savefig('data/insights/power_curve.png')
    print("Power curve plot saved to data/insights/power_curve.png")

    # 2. Power over Time (Sample)
    plt.figure(figsize=(10, 6))
    df['y (% relative to rated power)'].head(1000).plot(color='green')
    plt.title('Power Output Over Time (1000 Intervals)')
    plt.xlabel('Interval (10m)')
    plt.ylabel('Power Output (%)')
    plt.grid(True)
    plt.savefig('data/insights/power_over_time.png')
    print("Power over time plot saved to data/insights/power_over_time.png")

if __name__ == "__main__":
    main()
