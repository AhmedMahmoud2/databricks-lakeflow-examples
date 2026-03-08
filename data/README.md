# Sample Data

This directory would contain sample data files for testing the pipeline examples.

## Structure

```
data/
├── landing/          # Raw landing zone (simulated)
│   ├── sales/       # Sample sales JSON files
│   ├── customers/   # Sample customer data
│   └── products/    # Sample product catalog
├── schemas/         # Schema definitions
│   └── sales.json
└── README.md        # This file
```

## Using the Examples

To run the pipeline examples, you would:

1. Upload sample data to your Databricks workspace
2. Update the paths in the Python files to point to your data location
3. Create a Lakeflow Pipeline pointing to the `src/` directory
4. Run the pipeline

## Sample Data Format

### Sales Data (JSON)
```json
{
  "transaction_id": "TXN001",
  "transaction_date": "2024-01-15",
  "store_id": "STORE001",
  "product_id": "PROD001",
  "customer_id": "CUST001",
  "quantity": 2,
  "unit_price": 29.99,
  "amount": 59.98,
  "currency": "USD",
  "payment_method": "CREDIT_CARD"
}
```

### Customer Data (JSON)
```json
{
  "customer_id": "CUST001",
  "email": "customer@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "date_of_birth": "1985-05-15",
  "country": "US",
  "registration_date": "2023-01-01"
}
```

### Product Data (JSON)
```json
{
  "product_id": "PROD001",
  "product_name": "Widget A",
  "category": "ELECTRONICS",
  "subcategory": "GADGETS",
  "brand": "BrandX",
  "unit_price": 29.99,
  "unit_cost": 15.00,
  "is_active": true
}
```

## Generating Sample Data

You can generate sample data using Python:

```python
import json
from datetime import datetime, timedelta
import random

# Generate sample sales data
sales = []
for i in range(1000):
    sales.append({
        "transaction_id": f"TXN{i:06d}",
        "transaction_date": (datetime.now() - timedelta(days=random.randint(0,90))).strftime("%Y-%m-%d"),
        "store_id": f"STORE{random.randint(1,10):03d}",
        "product_id": f"PROD{random.randint(1,100):03d}",
        "customer_id": f"CUST{random.randint(1,500):04d}",
        "quantity": random.randint(1,5),
        "unit_price": round(random.uniform(10, 200), 2),
        "amount": 0,  # Calculate below
        "currency": "USD",
        "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "CASH", "DIGITAL_WALLET"])
    })
    sales[i]["amount"] = round(sales[i]["quantity"] * sales[i]["unit_price"], 2)

# Save to file
with open('sales.json', 'w') as f:
    for sale in sales:
        f.write(json.dumps(sale) + '\n')
```

---

*For production use, replace these samples with your actual data sources.*
