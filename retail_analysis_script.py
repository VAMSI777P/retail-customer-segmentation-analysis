# Sales Performance and Customer Segmentation Analysis
# End-to-End Data Analyst Project

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Set style for better visualizations
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

print("=== SALES PERFORMANCE AND CUSTOMER SEGMENTATION ANALYSIS ===")
print("Loading datasets...")

# Load the datasets
customers_df = pd.read_csv('customers_data.csv')
products_df = pd.read_csv('products_data.csv')
transactions_df = pd.read_csv('transactions_data.csv')

# Convert date columns
customers_df['join_date'] = pd.to_datetime(customers_df['join_date'])
transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])

print(f"Customers: {customers_df.shape[0]} records")
print(f"Products: {products_df.shape[0]} records")
print(f"Transactions: {transactions_df.shape[0]} records")

# ===== STEP 1: DATA PREPARATION =====
print("\n=== STEP 1: DATA PREPARATION ===")

# Merge datasets for comprehensive analysis
sales_data = transactions_df.merge(customers_df, on='customer_id')                            .merge(products_df, on='product_id')

# Create additional features
sales_data['transaction_month'] = sales_data['transaction_date'].dt.month
sales_data['transaction_year'] = sales_data['transaction_date'].dt.year
sales_data['customer_tenure_days'] = (sales_data['transaction_date'] - sales_data['join_date']).dt.days

print(f"Combined dataset shape: {sales_data.shape}")
print(f"Date range: {sales_data['transaction_date'].min()} to {sales_data['transaction_date'].max()}")

# ===== STEP 2: EXPLORATORY DATA ANALYSIS =====
print("\n=== STEP 2: EXPLORATORY DATA ANALYSIS ===")

# 2.1 Sales Trends Analysis
monthly_sales = sales_data.groupby(['transaction_year', 'transaction_month'])['total_amount'].agg(['sum', 'count']).reset_index()
monthly_sales.columns = ['year', 'month', 'total_revenue', 'transaction_count']
monthly_sales['period'] = monthly_sales['year'].astype(str) + '-' + monthly_sales['month'].astype(str).str.zfill(2)

print("\nMonthly Sales Summary:")
print(monthly_sales.tail())

# 2.2 Customer Analysis
customer_summary = sales_data.groupby('customer_id').agg({
    'transaction_id': 'count',
    'total_amount': ['sum', 'mean'],
    'transaction_date': ['min', 'max'],
    'product_category': lambda x: len(x.unique())
}).round(2)

customer_summary.columns = ['transaction_count', 'total_spent', 'avg_order_value', 
                           'first_purchase', 'last_purchase', 'category_diversity']

print(f"\nCustomer Statistics:")
print(f"Average transactions per customer: {customer_summary['transaction_count'].mean():.2f}")
print(f"Average total spent per customer: ${customer_summary['total_spent'].mean():.2f}")
print(f"Average order value: ${customer_summary['avg_order_value'].mean():.2f}")

# 2.3 Product Category Analysis
category_performance = sales_data.groupby('product_category').agg({
    'total_amount': ['sum', 'mean', 'count'],
    'quantity': 'sum'
}).round(2)

category_performance.columns = ['total_revenue', 'avg_order_value', 'transaction_count', 'total_quantity']
category_performance = category_performance.sort_values('total_revenue', ascending=False)

print("\nTop Product Categories by Revenue:")
print(category_performance)

# ===== STEP 3: CUSTOMER SEGMENTATION (RFM ANALYSIS) =====
print("\n=== STEP 3: CUSTOMER SEGMENTATION (RFM ANALYSIS) ===")

# Calculate RFM metrics
snapshot_date = sales_data['transaction_date'].max() + pd.Timedelta(days=1)

rfm = sales_data.groupby('customer_id').agg({
    'transaction_date': lambda x: (snapshot_date - x.max()).days,  # Recency
    'transaction_id': 'count',  # Frequency
    'total_amount': 'sum'  # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']

# Add customer information
rfm = rfm.merge(customers_df[['customer_id', 'customer_age', 'gender', 'location']], on='customer_id')

print(f"RFM Analysis completed for {len(rfm)} customers")
print("\nRFM Summary Statistics:")
print(rfm[['recency', 'frequency', 'monetary']].describe())

# Create RFM scores (1-5 scale)
rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1])  # Lower recency = higher score
rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5])
rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5])

# Combine RFM scores
rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)

# ===== STEP 4: K-MEANS CLUSTERING =====
print("\n=== STEP 4: K-MEANS CLUSTERING ===")

# Prepare data for clustering
features = ['recency', 'frequency', 'monetary']
X = rfm[features].copy()

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Apply K-means clustering
optimal_k = 4
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
rfm['cluster'] = kmeans.fit_predict(X_scaled)

# Define segment names based on characteristics
def assign_segment_name(row):
    if row['r_score'] >= 4 and row['f_score'] >= 4 and row['m_score'] >= 4:
        return 'Champions'
    elif row['r_score'] >= 3 and row['f_score'] >= 3:
        return 'Loyal Customers'  
    elif row['r_score'] >= 4 and row['f_score'] <= 2:
        return 'Potential Loyalists'
    elif row['r_score'] <= 2:
        return 'At Risk'
    else:
        return 'Others'

rfm['segment_name'] = rfm.apply(assign_segment_name, axis=1)

# Segment profiling
segment_profile = rfm.groupby('segment_name').agg({
    'recency': 'mean',
    'frequency': 'mean', 
    'monetary': 'mean',
    'customer_id': 'count',
    'customer_age': 'mean'
}).round(2)

segment_profile.columns = ['avg_recency', 'avg_frequency', 'avg_monetary', 'customer_count', 'avg_age']
segment_profile['percentage'] = (segment_profile['customer_count'] / len(rfm) * 100).round(1)

print("\nCustomer Segment Profiles:")
print(segment_profile)

# ===== STEP 5: STATISTICAL ANALYSIS =====
print("\n=== STEP 5: STATISTICAL ANALYSIS ===")

# Test if there's significant difference in spending between segments
segment_spending = [rfm[rfm['segment_name'] == segment]['monetary'].values 
                   for segment in rfm['segment_name'].unique()]

f_stat, p_value = stats.f_oneway(*segment_spending)
print(f"ANOVA Test for spending differences between segments:")
print(f"F-statistic: {f_stat:.4f}")
print(f"P-value: {p_value:.4f}")

if p_value < 0.05:
    print("Result: Significant difference in spending between segments")
else:
    print("Result: No significant difference in spending between segments")

# Correlation analysis
correlation_matrix = rfm[['recency', 'frequency', 'monetary', 'customer_age']].corr()
print("\nCorrelation Matrix:")
print(correlation_matrix.round(3))

# ===== STEP 6: BUSINESS INSIGHTS =====
print("\n=== STEP 6: BUSINESS INSIGHTS ===")

# Calculate key business metrics
total_revenue = sales_data['total_amount'].sum()
total_customers = sales_data['customer_id'].nunique()
avg_order_value = sales_data['total_amount'].mean()
repeat_customers = len(rfm[rfm['frequency'] > 1])
repeat_rate = (repeat_customers / total_customers) * 100

print(f"Key Business Metrics:")
print(f"- Total Revenue: ${total_revenue:,.2f}")
print(f"- Total Customers: {total_customers:,}")
print(f"- Average Order Value: ${avg_order_value:.2f}")
print(f"- Repeat Customers: {repeat_customers:,}")
print(f"- Repeat Purchase Rate: {repeat_rate:.1f}%")

# Segment-specific insights
print("\nSegment-Specific Insights:")
for segment in segment_profile.index:
    segment_data = segment_profile.loc[segment]
    print(f"\n{segment}:")
    print(f"  - Customer Count: {segment_data['customer_count']} ({segment_data['percentage']}%)")
    print(f"  - Average Recency: {segment_data['avg_recency']:.1f} days")
    print(f"  - Average Frequency: {segment_data['avg_frequency']:.1f} purchases")
    print(f"  - Average Monetary Value: ${segment_data['avg_monetary']:.2f}")

# ===== STEP 7: SAVE RESULTS =====
print("\n=== STEP 7: SAVING RESULTS ===")

# Save the RFM analysis results
rfm.to_csv('customer_segments.csv', index=False)

# Save segment profiles
segment_profile.to_csv('segment_profiles.csv')

# Save monthly sales trends
monthly_sales.to_csv('monthly_sales_trends.csv', index=False)

# Save category performance
category_performance.to_csv('category_performance.csv')

print("Analysis completed! Files saved:")
print("- customer_segments.csv")
print("- segment_profiles.csv") 
print("- monthly_sales_trends.csv")
print("- category_performance.csv")

print("\n=== ANALYSIS COMPLETE ===")
print("Next Steps:")
print("1. Create visualizations in Power BI using the generated CSV files")
print("2. Develop targeted marketing strategies for each customer segment")
print("3. Implement A/B testing for segment-specific campaigns")
print("4. Monitor segment performance over time")
