"""
Generate comprehensive SAP seed data for Data Vault testing
Creates realistic data with multiple load dates to test incremental loads
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)  # For reproducibility

# Configuration
NUM_CUSTOMERS = 300
NUM_MATERIALS = 300
NUM_ORDERS = 2000
MIN_ITEMS_PER_ORDER = 2
MAX_ITEMS_PER_ORDER = 12

# Load dates for incremental testing
LOAD_DATES = [
    datetime(2024, 1, 1),
    datetime(2024, 3, 15),
    datetime(2024, 6, 1),
    datetime(2024, 9, 1),
    datetime(2024, 12, 1),
    datetime(2025, 1, 1),
]

# Company name templates
COMPANY_NAMES = [
    "Global", "International", "Worldwide", "European", "American", "Asian",
    "Pacific", "Atlantic", "Continental", "United", "Premier", "Elite",
    "Premium", "Advanced", "Modern", "Digital", "Tech", "Systems", "Solutions",
    "Industries", "Group", "Holdings", "Corporation", "Enterprises", "Trading",
    "Distribution", "Supply", "Commerce", "Business", "Partners"
]

COMPANY_SUFFIXES = [
    "GmbH", "Ltd", "Inc", "LLC", "SA", "SRL", "SARL", "AG", "Pty", "Ltd",
    "Co", "Corp", "LLP", "PLC", "BV", "NV", "KG", "Sdn Bhd", "Pvt Ltd"
]

CITIES = {
    'DE': ['Berlin', 'Munich', 'Hamburg', 'Frankfurt', 'Stuttgart', 'Cologne', 'Düsseldorf'],
    'GB': ['London', 'Manchester', 'Birmingham', 'Glasgow', 'Edinburgh', 'Liverpool'],
    'US': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami', 'Seattle', 'Boston'],
    'FR': ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Bordeaux'],
    'IT': ['Milan', 'Rome', 'Naples', 'Turin', 'Florence', 'Venice'],
    'ES': ['Madrid', 'Barcelona', 'Valencia', 'Seville', 'Bilbao'],
    'NL': ['Amsterdam', 'Rotterdam', 'The Hague', 'Utrecht', 'Eindhoven'],
    'BE': ['Brussels', 'Antwerp', 'Ghent', 'Bruges', 'Liège'],
    'CH': ['Zurich', 'Geneva', 'Basel', 'Bern', 'Lausanne'],
    'AT': ['Vienna', 'Graz', 'Linz', 'Salzburg', 'Innsbruck'],
}

# Countries and regions
COUNTRIES = {
    'DE': {'regions': ['16', 'BY', 'HH', 'HE', 'BW', 'NW'], 'currency': 'EUR'},
    'GB': {'regions': ['ENG', 'SCT', 'WLS'], 'currency': 'GBP'},
    'US': {'regions': ['NY', 'CA', 'TX', 'FL', 'IL'], 'currency': 'USD'},
    'FR': {'regions': ['75', '13', '69', '31', '06'], 'currency': 'EUR'},
    'IT': {'regions': ['MI', 'RM', 'NA', 'TO'], 'currency': 'EUR'},
    'ES': {'regions': ['MD', 'B', 'CT'], 'currency': 'EUR'},
    'NL': {'regions': ['NH', 'ZH', 'UT'], 'currency': 'EUR'},
    'BE': {'regions': ['BRU', 'VAN', 'OVL'], 'currency': 'EUR'},
    'CH': {'regions': ['ZH', 'GE', 'BS'], 'currency': 'CHF'},
    'AT': {'regions': ['9', '1', '7'], 'currency': 'EUR'},
}

# Material types and groups
MATERIAL_TYPES = ['HAWA', 'FERT', 'ROH', 'DIEN', 'VERP']
MATERIAL_GROUPS = ['001', '002', '003', '004', '005']
UNITS = ['EA', 'ST', 'KG', 'BOX', 'PAL', 'ROL']

# Order types
ORDER_TYPES = ['TA', 'OR', 'ZRE', 'ZCR']

def generate_company_name():
    """Generate a random company name"""
    prefix = random.choice(COMPANY_NAMES)
    suffix = random.choice(COMPANY_SUFFIXES)
    if random.random() < 0.3:
        middle = random.choice(['Trading', 'Supply', 'Distribution', 'Commerce'])
        return f"{prefix} {middle} {suffix}"
    return f"{prefix} {suffix}"

def generate_phone(country):
    """Generate phone number based on country"""
    codes = {
        'DE': '+49', 'GB': '+44', 'US': '+1', 'FR': '+33', 'IT': '+39',
        'ES': '+34', 'NL': '+31', 'BE': '+32', 'CH': '+41', 'AT': '+43'
    }
    code = codes.get(country, '+1')
    number = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    return f"{code}{number}"

def generate_customers():
    """Generate customer master data (KNA1)"""
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_num = f"0001{i:06d}"  # Format: 0001XXXXXX (10 digits)
        country = random.choice(list(COUNTRIES.keys()))
        region = random.choice(COUNTRIES[country]['regions'])
        city = random.choice(CITIES.get(country, ['City']))
        
        # Some customers get updated in later loads (for satellite testing)
        base_load_date = random.choice(LOAD_DATES[:4])  # Earlier loads
        
        customers.append({
            'KUNNR': customer_num,
            'NAME1': generate_company_name(),
            'ORT01': city,
            'LAND1': country,
            'REGIO': region,
            'PSTLZ': f"{random.randint(10000, 99999)}",
            'STRAS': f"Street {random.randint(1, 999)}",
            'TELF1': generate_phone(country),
            'KTOK': 'KUND',
            'RECORD_SOURCE': 'SAP',
            'LOAD_DATE': base_load_date.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Add updated version for some customers (30% chance)
        if random.random() < 0.3 and base_load_date < LOAD_DATES[-2]:
            updated_load_date = random.choice([d for d in LOAD_DATES if d > base_load_date])
            customers.append({
                'KUNNR': customer_num,
                'NAME1': generate_company_name(),  # Changed name
                'ORT01': city,
                'LAND1': country,
                'REGIO': region,
                'PSTLZ': f"{random.randint(10000, 99999)}",  # Changed postal code
                'STRAS': f"Street {random.randint(1, 999)}",  # Changed address
                'TELF1': generate_phone(country),  # Changed phone
                'KTOK': 'KUND',
                'RECORD_SOURCE': 'SAP',
                'LOAD_DATE': updated_load_date.strftime('%Y-%m-%d %H:%M:%S')
            })
    
    return customers

def generate_materials():
    """Generate material master data (MARA)"""
    materials = []
    for i in range(1, NUM_MATERIALS + 1):
        material_num = f"{i:018d}"
        mat_type = random.choice(MATERIAL_TYPES)
        mat_group = random.choice(MATERIAL_GROUPS)
        unit = random.choice(UNITS)
        
        # Weight and volume based on material type
        if mat_type == 'ROH':
            gross_weight = random.uniform(500, 5000)
            net_weight = gross_weight * 0.95
            volume = random.uniform(5, 30)
        elif mat_type == 'FERT':
            gross_weight = random.uniform(50, 500)
            net_weight = gross_weight * 0.93
            volume = random.uniform(0.5, 5)
        elif mat_type == 'DIEN':
            gross_weight = 0
            net_weight = 0
            volume = 0
        else:
            gross_weight = random.uniform(1, 100)
            net_weight = gross_weight * 0.92
            volume = random.uniform(0.01, 0.5)
        
        base_load_date = random.choice(LOAD_DATES[:4])
        creation_date = (base_load_date - timedelta(days=random.randint(365, 1095))).strftime('%Y-%m-%d')
        
        materials.append({
            'MATNR': material_num,
            'MATKL': mat_group,
            'MTART': mat_type,
            'MEINS': unit,
            'BRGEW': f"{gross_weight:.3f}",
            'NTGEW': f"{net_weight:.3f}",
            'VOLUM': f"{volume:.3f}",
            'ERSDA': creation_date,
            'LAEDA': (base_load_date - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
            'VPSTA': 'KDEV',
            'RECORD_SOURCE': 'SAP',
            'LOAD_DATE': base_load_date.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Add updated version for some materials (25% chance)
        if random.random() < 0.25 and base_load_date < LOAD_DATES[-2]:
            updated_load_date = random.choice([d for d in LOAD_DATES if d > base_load_date])
            materials.append({
                'MATNR': material_num,
                'MATKL': mat_group,
                'MTART': mat_type,
                'MEINS': unit,
                'BRGEW': f"{gross_weight * random.uniform(0.9, 1.1):.3f}",  # Changed weight
                'NTGEW': f"{net_weight * random.uniform(0.9, 1.1):.3f}",
                'VOLUM': f"{volume * random.uniform(0.9, 1.1):.3f}",
                'ERSDA': creation_date,
                'LAEDA': (updated_load_date - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
                'VPSTA': 'KDEV',
                'RECORD_SOURCE': 'SAP',
                'LOAD_DATE': updated_load_date.strftime('%Y-%m-%d %H:%M:%S')
            })
    
    return materials

def generate_orders(customers, materials):
    """Generate sales orders (VBAK) and order items (VBAP)"""
    orders = []
    order_items = []
    partner_functions = []
    
    order_num = 10000001
    
    for load_date_idx, load_date in enumerate(LOAD_DATES):
        # Generate orders for this load date
        orders_per_load = NUM_ORDERS // len(LOAD_DATES)
        if load_date_idx == len(LOAD_DATES) - 1:
            orders_per_load = NUM_ORDERS - sum(NUM_ORDERS // len(LOAD_DATES) for _ in range(len(LOAD_DATES) - 1))
        
        for _ in range(orders_per_load):
            order_num_str = f"{order_num:010d}"
            customer = random.choice(customers)
            customer_num = customer['KUNNR']
            country = customer['LAND1']
            currency = COUNTRIES.get(country, {}).get('currency', 'EUR')
            
            # Order date within last 2 years from load date
            order_date = load_date - timedelta(days=random.randint(1, 730))
            document_date = order_date - timedelta(days=random.randint(0, 5))
            
            # Order value
            net_value = random.uniform(10000, 500000)
            
            orders.append({
                'VBELN': order_num_str,
                'KUNNR': customer_num,
                'ERDAT': order_date.strftime('%Y-%m-%d'),
                'AUDAT': document_date.strftime('%Y-%m-%d'),
                'NETWR': f"{net_value:.2f}",
                'WAERK': currency,
                'VKORG': '1000',
                'VTWEG': '01',
                'SPART': 'Z1',
                'VKBUR': '1000',
                'VKGRP': '001',
                'AUART': random.choice(ORDER_TYPES),
                'RECORD_SOURCE': 'SAP',
                'LOAD_DATE': load_date.strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Partner function
            partner_functions.append({
                'VBELN': order_num_str,
                'PARVW': 'AG',
                'KUNNR': customer_num,
                'PERNR': '',
                'LIFNR': '',
                'ADRNR': customer_num.lstrip('0'),
                'RECORD_SOURCE': 'SAP',
                'LOAD_DATE': load_date.strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Order items
            num_items = random.randint(MIN_ITEMS_PER_ORDER, MAX_ITEMS_PER_ORDER)
            item_num = 10
            
            for _ in range(num_items):
                material = random.choice(materials)
                material_num = material['MATNR']
                quantity = random.uniform(1, 200)
                unit_price = random.uniform(10, 5000)
                net_value_item = quantity * unit_price
                
                order_items.append({
                    'VBELN': order_num_str,
                    'POSNR': f"{item_num:06d}",
                    'MATNR': material_num,
                    'KWMENG': f"{quantity:.3f}",
                    'VRKME': material['MEINS'],
                    'NETWR': f"{net_value_item:.2f}",
                    'WAERK': currency,
                    'WERKS': '1000',
                    'LGORT': '0001',
                    'RECORD_SOURCE': 'SAP',
                    'LOAD_DATE': load_date.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                item_num += 10
            
            order_num += 1
    
    return orders, order_items, partner_functions

def write_csv(filename, data, fieldnames):
    """Write data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    print("Generating customer data...")
    customers = generate_customers()
    print(f"Generated {len(customers)} customer records")
    
    print("Generating material data...")
    materials = generate_materials()
    print(f"Generated {len(materials)} material records")
    
    print("Generating order data...")
    orders, order_items, partner_functions = generate_orders(customers, materials)
    print(f"Generated {len(orders)} orders and {len(order_items)} order items")
    
    print("Writing CSV files...")
    write_csv('seeds/seed_sap_kna1.csv', customers, 
              ['KUNNR', 'NAME1', 'ORT01', 'LAND1', 'REGIO', 'PSTLZ', 'STRAS', 'TELF1', 'KTOK', 'RECORD_SOURCE', 'LOAD_DATE'])
    
    write_csv('seeds/seed_sap_mara.csv', materials,
              ['MATNR', 'MATKL', 'MTART', 'MEINS', 'BRGEW', 'NTGEW', 'VOLUM', 'ERSDA', 'LAEDA', 'VPSTA', 'RECORD_SOURCE', 'LOAD_DATE'])
    
    write_csv('seeds/seed_sap_vbak.csv', orders,
              ['VBELN', 'KUNNR', 'ERDAT', 'AUDAT', 'NETWR', 'WAERK', 'VKORG', 'VTWEG', 'SPART', 'VKBUR', 'VKGRP', 'AUART', 'RECORD_SOURCE', 'LOAD_DATE'])
    
    write_csv('seeds/seed_sap_vbap.csv', order_items,
              ['VBELN', 'POSNR', 'MATNR', 'KWMENG', 'VRKME', 'NETWR', 'WAERK', 'WERKS', 'LGORT', 'RECORD_SOURCE', 'LOAD_DATE'])
    
    write_csv('seeds/seed_sap_vbpa.csv', partner_functions,
              ['VBELN', 'PARVW', 'KUNNR', 'PERNR', 'LIFNR', 'ADRNR', 'RECORD_SOURCE', 'LOAD_DATE'])
    
    print("Done!")
    print(f"\nSummary:")
    print(f"  Customers: {len(customers)} records")
    print(f"  Materials: {len(materials)} records")
    print(f"  Orders: {len(orders)} records")
    print(f"  Order Items: {len(order_items)} records")
    print(f"  Partner Functions: {len(partner_functions)} records")

if __name__ == '__main__':
    main()
