import React, { useState, useEffect } from "react";
import axios from "axios";
import { ChevronDown, ChevronUp } from "lucide-react";
import styles from "./OrderListForSupplier.module.css";

function OrderListForSupplier({ supplierId }) {
  const [orders, setOrders] = useState([]);
  const [expandedOrders, setExpandedOrders] = useState(new Set());
  const [displayHistory, setDisplayHistory] = useState(false);

  useEffect(() => {
    axios
      .post("http://localhost:3000/order/by-supplier", {
        supplier_id: supplierId,
      })
      .then((res) => setOrders(res.data))
      .catch((err) => console.error("Failed to fetch orders", err));
  }, [supplierId]);

  const toggleOrder = (orderId) => {
    setExpandedOrders((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(orderId)) {
        newSet.delete(orderId);
      } else {
        newSet.add(orderId);
      }
      return newSet;
    });
  };

  const isExpanded = (orderId) => expandedOrders.has(orderId);
  const handleOrderArrivalConfirmation = async (orderId) => {
    try {
      const res = await axios.put(
        `http://localhost:3000/order/update-status/${orderId}`,
        {
          status: "בתהליך",
        },
      );

      setOrders((prevOrders) =>
        prevOrders.map((order) =>
          order.order_id === orderId ? { ...order, status: "בתהליך" } : order,
        ),
      );

      console.log(`✅ Order ${orderId} status updated:`, res.data);
    } catch (err) {
      console.error("❌ Failed to update order status:", err);
    }
  };

  const displayCompleteOrders = async () => {
    setDisplayHistory((prev) => !prev);
  };

  return (
    <div className={styles.ordersSection}>
      <h2 className={styles.title}>רשימת הזמנות לספק</h2>
      <button className={styles.historyButton} onClick={displayCompleteOrders}>
        {displayHistory
          ? "לצפיה בהזמנות שטרם סופקו"
          : "לצפיה בהיסטורית ההזמנות"}
      </button>
      <div className={styles.orderHeaderRow}>
        <span>מס' הזמנה</span>
        <span>תאריך</span>
        <span>סטטוס</span>
        <span></span> 
      </div>

      <div className={styles.orderList}>
        {orders
          .filter((order) =>
            displayHistory
              ? order.status === "הושלמה"
              : order.status !== "הושלמה",
          )
          .map((order) => (
            <div key={order.order_id} className={styles.orderRow}>
              <div
                className={styles.orderHeader}
                onClick={() => toggleOrder(order.order_id)}
              >
                <span className={styles.chevron}>
                  {isExpanded(order.order_id) ? (
                    <ChevronUp size={20} />
                  ) : (
                    <ChevronDown size={20} />
                  )}
                </span>
                <div className={styles.orderAction}>
                  {order.status === "בוצעה" && (
                    <button
                      className={styles.confirmButton}
                      onClick={() =>
                        handleOrderArrivalConfirmation(order.order_id)
                      }
                    >
                      לאישור קבלת הזמנה
                    </button>
                  )}

                  {order.status === "בתהליך" && (
                    <button className={styles.confirmButton} disabled>
                      ההזמנה אושרה
                    </button>
                  )}

                  {order.status === "הושלמה" && <div>ההזמנה הושלמה</div>}
                </div>
                <span>{order.status}</span>
                <span>{new Date(order.created_date).toLocaleDateString()}</span>
                <span>#{order.order_id}</span>
              </div>

              {isExpanded(order.order_id) && (
                <div className={styles.orderDetails}>
                  <p>
                    <strong>מוצרים:</strong>
                  </p>
                  <ul>
                    {order.products.map((product) => (
                      <li key={product.product_id}>
                        מספר מוצר: {product.product_id}, כמות:{" "}
                        {product.quantity}, שם מוצר: {product.product_name}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          ))}
      </div>
    </div>
  );
}

export default OrderListForSupplier;
