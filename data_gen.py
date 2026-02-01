import pandas as pd
import numpy as np
import random

# Configuration
n_rows = 50000  # Adjust as needed
output_file = 'offer_sup_dat.csv'

months = ['202501', '202502', '202503', '202504', '202505', '202506']
bus = ['Mobile', 'Broadband', 'TV']
tiers = ['Gold', 'Silver', 'Bronze']
sites = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai']
offer_types = ['Discount', 'Cashback', 'Loyalty', 'Bundle', 'Speed-Boost']
offer_suffixes = ['Promo', 'Special', 'Retention', 'Welcome', 'Upgrade']

# Step 1: Pre-generate master volumes for every possible segment
# This ensures that 'Jan-Mobile-Gold-Mumbai' has ONE fixed handled count
segment_master_handled = {}
for m in months:
    for b in bus:
        for t in tiers:
            for s in sites:
                key = (m, b, t, s)
                # Assign a fixed total volume for this specific bucket
                segment_master_handled[key] = random.randint(1000, 5000)

data = []

# Step 2: Generate the rows
for _ in range(n_rows):
    mth = random.choice(months)
    bu = random.choice(bus)
    tier = random.choice(tiers)
    site = random.choice(sites)
    
    # Retrieve the fixed handled count for this segment
    handled = segment_master_handled[(mth, bu, tier, site)]
    
    # Generate random offer combination (The "Pipe" format)
    num_offers = random.randint(1, 3)
    current_offers = [f"{random.choice(offer_types)}-{random.choice(offer_suffixes)}" for _ in range(num_offers)]
    agg_pres = "|".join(list(set(current_offers)))
    
    # Simulate funnel success (Subset logic)
    pres_list = agg_pres.split('|')
    
    # Extended: subset of presented (60% likelihood of extending something)
    ext_list = random.sample(pres_list, k=random.randint(0, len(pres_list))) if random.random() > 0.4 else []
    agg_ext = "|".join(ext_list)
    
    # Accepted: subset of extended (40% likelihood of accepting something)
    acc_list = random.sample(ext_list, k=random.randint(0, len(ext_list))) if (ext_list and random.random() > 0.6) else []
    agg_acc = "|".join(acc_list)
    
    # The count for this specific row (subset of handled)
    # In reality, the sum of all 'Counts' for a segment should be <= Handled
    count = random.randint(5, 50)
    
    data.append([mth, bu, tier, site, handled, agg_pres, agg_ext, agg_acc, count])

# Create DataFrame
df = pd.DataFrame(data, columns=['Mth', 'BU', 'Tier', 'Site', 'Handled', 'Pres', 'Ext', 'Acc', 'Count'])

# Final Step: Save to CSV
df.to_csv(output_file, index=False)
print(f"Successfully generated {output_file} with consistent segment denominators.")