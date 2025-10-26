import pandas as pd
import numpy as np
import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker

# initialize faker
fake = Faker()

# --- configuration ---
NUM_BATCHES = 2
WAFERS_PER_BATCH = 25
SENSOR_READINGS_PER_STEP = 100 # number of sensor readings per process step

# define data output directory
OUTPUT_DIR = './data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 1. generate dimension data ---
print("Generating dimension tables...")

# equipment dimension
equipment_data = {
    'equipment_id': ['ETCH_TOOL_01', 'ETCH_TOOL_02', 'LITHO_TOOL_01', 'LITHO_TOOL_02', 'DEP_TOOL_01', 'DEP_TOOL_02', 'CMP_TOOL_01'],
    'tool_type': ['Etcher', 'Etcher', 'Lithography', 'Lithography', 'Deposition', 'Deposition', 'CMP'],
    'model_name': ['Kiyo_CX', 'Kiyo_VX', 'NXT:2000i', 'NXT:1980Di', 'Vector_Extreme', 'Vector_Excel', 'Reflexion_LK'],
    'install_date': [fake.date_this_decade() for _ in range(7)]
}
df_equipment = pd.DataFrame(equipment_data)
df_equipment.to_csv(os.path.join(OUTPUT_DIR, 'dim_equipment.csv'), index=False)

# process steps dimension
# Process Steps Dimension
process_steps_data = {
    'process_step_id': [1, 2, 3, 4, 5],
    'step_name': ['Lithography', 'Etch', 'Deposition', 'CMP', 'Inspection'],
    'target_duration_sec': [120, 180, 240, 150, 90]
}
df_process_steps = pd.DataFrame(process_steps_data)
df_process_steps.to_csv(os.path.join(OUTPUT_DIR, 'dim_process_steps.csv'), index=False)

# --- 2. Generate Transactional (Fact) Data ---
print("Generating transactional data...")
wafer_logs_list = []
sensor_readings_list = []
yield_results_list = []

# get equipment IDs by type for easier assignment
equipment_map = {
    'Lithography': ['LITHO_TOOL_01', 'LITHO_TOOL_02'],
    'Etch': ['ETCH_TOOL_01', 'ETCH_TOOL_02'],
    'Deposition': ['DEP_TOOL_01', 'DEP_TOOL_02'],
    'CMP': ['CMP_TOOL_01'],
    'Inspection': ['INSPECT_01'] # Add an inspection tool
}
# Add inspection tool to main equipment list
df_equipment = pd.concat([df_equipment, pd.DataFrame({
    'equipment_id': ['INSPECT_01'], 'tool_type': ['Inspection'], 
    'model_name': ['Sirius_HRP'], 'install_date': [fake.date_this_decade()]
})], ignore_index=True)
df_equipment.to_csv(os.path.join(OUTPUT_DIR, 'dim_equipment.csv'), index=False)

current_time = datetime.now() - timedelta(days=1)

for batch_i in range(NUM_BATCHES):
    batch_id = f'B-{1000 + batch_i}'

    for wafer_j in range(WAFERS_PER_BATCH):
        wafer_id = f'W-{batch_id}-{100 + wafer_j}'
        wafer_process_log = []

        wafer_start_time = current_time + timedelta(minutes=wafer_j * 20)
        step_start_time = wafer_start_time

        # base yield - introduce some variance
        base_yield = 95.0 - (random.random() * 5)
        failure_reason = "None"

        # --- Simulate Process Flow ---
        for step_id, step_name, target_duration in df_process_steps.itertuples(index=False):
            # pick a tool for this step
            tool_id = random.choice(equipment_map[step_name])

            # Simulate process duration (target +/- 10%)
            duration = int(target_duration * (1 + (random.random() - 0.5) * 0.2))
            step_end_time = step_start_time + timedelta(seconds=duration)

            # --- Generate Sensor Data for this step ---
            anomaly_this_step = False
            if random.random() < 0.1: # 10% chance of an anomaly
                anomaly_this_step = True
                base_yield -= (random.random() * 10) # anomaly impacts yield
                failure_reason = f"{step_name}_Anomaly"

                for _ in range(SENSOR_READINGS_PER_STEP):
                    reading_time = step_start_time + timedelta(seconds=random.randint(0, duration))

                    # normal sensor values
                    pressure = 50 + (random.random() - 0.5) * 2
                    rf_power = 1000 + (random.random() - 0.5) * 20
                    gas_flow = 100 + (random.random() - 0.5) * 5

                    # inject anomaly
                    if anomaly_this_step and random.random() < 0.3:
                        pressure *= (1 + random.random()) # pressure spike
                        rf_power *= (1 - random.random() * 0.5) # power drop

                    sensor_readings_list.append({
                        'timestamp': reading_time.isoformat(),
                        'equipment_id': tool_id,
                        'wafer_id': wafer_id,
                        'sensor_name': 'Pressure_Torr',
                        'sensor_value': pressure
                    })
                    sensor_readings_list.append({
                        'timestamp': reading_time.isoformat(),
                        'equipment_id': tool_id,
                        'wafer_id': wafer_id,
                        'sensor_name': 'RF_Power_W',
                        'sensor_value': rf_power
                    })
                    sensor_readings_list.append({
                        'timestamp': reading_time.isoformat(),
                        'equipment_id': tool_id,
                        'wafer_id': wafer_id,
                        'sensor_name': 'Gas_Flow_sccm',
                        'sensor_value': gas_flow
                    })

                # append to wafer log (json structure)
                wafer_process_log.append({
                    'step_id': step_id,
                    'step_name': step_name,
                    'equipment_id': tool_id,
                    'start_time': step_start_time.isoformat(),
                    'end_time': step_end_time.isoformat(),
                    'duration_sec': duration
                })

                step_start_time = step_end_time  # next step starts when this one ends

            # add to overall wafer logs
            wafer_logs_list.append({
                'wafer_id': wafer_id,
                'batch_id': batch_id,
                'start_time': wafer_start_time.isoformat(),
                'end_time': step_end_time.isoformat(),
                'process_flow': wafer_process_log
            })

            # --- Generate Final Yield Result ---
            yield_results_list.append({
                'wafer_id': wafer_id,
                'batch_id': batch_id,
                'final_yield_percentage': max(0, base_yield), # Ensure yield isn't negative
                'failure_reason_code': failure_reason
            })
    
print("Saving transactional data...")

# Save sensor readings (CSV)
df_sensor_readings = pd.DataFrame(sensor_readings_list)
df_sensor_readings.to_csv(os.path.join(OUTPUT_DIR, f'sensor_readings_{current_time.strftime("%Y%m%d")}.csv'), index=False)

# Save Yield Results (CSV)
df_yield_results = pd.DataFrame(yield_results_list)
df_yield_results.to_csv(os.path.join(OUTPUT_DIR, f'yield_results_{current_time.strftime("%Y%m%d")}.csv'), index=False)

# Save Wafer Logs (JSON)
with open(os.path.join(OUTPUT_DIR, f'wafer_logs_{current_time.strftime("%Y%m%d")}.json'), 'w') as f:
    json.dump(wafer_logs_list, f, indent=2)

print(f"Data generation complete. Files saved to {OUTPUT_DIR}")
