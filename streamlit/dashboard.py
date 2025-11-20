import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine

# Page config
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)

# Connect to PostgreSQL
@st.cache_resource
def get_connection():
    return create_engine(
        "postgresql://airflow:airflow@localhost:5432/ecommerce"
    )

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_data(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return df

# Header
st.title("E-Commerce Real-Time Analytics Dashboard")
st.markdown("### Brazilian E-Commerce Data Pipeline - Live Metrics")
st.markdown("---")

# Refresh button
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Metrics Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_orders = load_data("SELECT COUNT(DISTINCT order_id) as cnt FROM analytics.fact_orders")
    st.metric(
        label="Total Orders",
        value=f"{total_orders['cnt'][0]:,}",
        delta="Live"
    )

with col2:
    total_revenue = load_data("SELECT SUM(total_price) as revenue FROM analytics.fact_orders")
    revenue_val = total_revenue['revenue'][0] if total_revenue['revenue'][0] else 0
    st.metric(
        label="Total Revenue",
        value=f"R$ {revenue_val:,.2f}",
        delta="BRL"
    )

with col3:
    avg_order = load_data("SELECT AVG(total_price) as avg FROM analytics.fact_orders")
    avg_val = avg_order['avg'][0] if avg_order['avg'][0] else 0
    st.metric(
        label="Avg Order Value",
        value=f"R$ {avg_val:,.2f}"
    )

with col4:
    total_customers = load_data("SELECT COUNT(DISTINCT customer_id) as cnt FROM analytics.fact_orders")
    st.metric(
        label="Total Customers",
        value=f"{total_customers['cnt'][0]:,}"
    )

st.markdown("---")

# Two column layout for charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Revenue Trend")
    revenue_data = load_data("""
        SELECT 
            date, 
            SUM(total_revenue) as revenue,
            SUM(num_orders) as orders
        FROM analytics.agg_daily_revenue
        GROUP BY date
        ORDER BY date
    """)
    
    if not revenue_data.empty:
        fig = px.line(
            revenue_data, 
            x='date', 
            y='revenue',
            title="Revenue Over Time",
            labels={'revenue': 'Revenue (R$)', 'date': 'Date'}
        )
        fig.update_traces(line_color='#1f77b4', line_width=3)
        fig.update_layout(
            hovermode='x unified',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue data available yet. Run the Airflow DAG to generate data.")

with col2:
    st.subheader("Revenue by State")
    state_data = load_data("""
        SELECT 
            customer_state, 
            SUM(total_revenue) as revenue,
            SUM(num_orders) as orders
        FROM analytics.agg_daily_revenue
        GROUP BY customer_state
        ORDER BY revenue DESC
        LIMIT 10
    """)
    
    if not state_data.empty:
        fig = px.bar(
            state_data, 
            x='customer_state', 
            y='revenue',
            title="Top 10 States by Revenue",
            labels={'revenue': 'Revenue (R$)', 'customer_state': 'State'},
            color='revenue',
            color_continuous_scale='Blues'
        )
        fig.update_layout(
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No state data available yet.")

# Second row of visualizations
col1, col2 = st.columns(2)

with col1:
    st.subheader("Orders by State")
    orders_by_state = load_data("""
        SELECT 
            customer_state,
            SUM(num_orders) as total_orders
        FROM analytics.agg_daily_revenue
        GROUP BY customer_state
        ORDER BY total_orders DESC
        LIMIT 10
    """)
    
    if not orders_by_state.empty:
        fig = px.pie(
            orders_by_state,
            values='total_orders',
            names='customer_state',
            title='Order Distribution by Top 10 States',
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No order data available yet.")

with col2:
    st.subheader("Average Order Value by State")
    avg_by_state = load_data("""
        SELECT 
            customer_state,
            AVG(avg_order_value) as avg_value
        FROM analytics.agg_daily_revenue
        GROUP BY customer_state
        ORDER BY avg_value DESC
        LIMIT 10
    """)
    
    if not avg_by_state.empty:
        fig = px.bar(
            avg_by_state,
            x='avg_value',
            y='customer_state',
            orientation='h',
            title='Average Order Value by State',
            labels={'avg_value': 'Avg Order Value (R$)', 'customer_state': 'State'},
            color='avg_value',
            color_continuous_scale='Greens'
        )
        fig.update_layout(
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No average order value data available yet.")

st.markdown("---")

# Recent Orders Table
st.subheader("Recent Orders Stream")

# Add date filter
col1, col2 = st.columns([3, 1])
with col2:
    limit = st.selectbox("Show records:", [10, 20, 50, 100], index=1)

recent_orders = load_data(f"""
    SELECT 
        order_id,
        customer_city,
        customer_state,
        total_price,
        total_freight,
        num_products,
        order_status,
        order_date
    FROM analytics.fact_orders
    ORDER BY order_date DESC
    LIMIT {limit}
""")

if not recent_orders.empty:
    # Format the dataframe
    recent_orders['total_price'] = recent_orders['total_price'].apply(lambda x: f"R$ {x:,.2f}")
    recent_orders['total_freight'] = recent_orders['total_freight'].apply(lambda x: f"R$ {x:,.2f}")
    recent_orders['order_date'] = pd.to_datetime(recent_orders['order_date']).dt.strftime('%Y-%m-%d %H:%M')
    
    st.dataframe(
        recent_orders,
        use_container_width=True,
        hide_index=True,
        column_config={
            "order_id": "Order ID",
            "customer_city": "City",
            "customer_state": "State",
            "total_price": "Total Price",
            "total_freight": "Freight",
            "num_products": st.column_config.NumberColumn("Products", format="%d"),
            "order_status": "Status",
            "order_date": "Order Date"
        }
    )
else:
    st.info("No orders found. Run the Airflow DAG to generate order data.")

# Footer
st.markdown("---")
st.markdown("""
    **Data Pipeline:** Kafka → PostgreSQL → dbt → Streamlit  
    **Last Updated:** {} UTC  
    **Source:** Olist Brazilian E-Commerce Dataset
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))