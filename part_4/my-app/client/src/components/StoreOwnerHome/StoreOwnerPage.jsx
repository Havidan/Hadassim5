import React, { useState, useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom"; 
import styles from "./StoreOwnerPage.module.css";
import SuppliersList from "../SuppliersList/SuppliersList"; 
import OrderListForOwner from "../OrderListForOwner/OrderListForOwner";

function StoreOwnerPage() {
  const navigate = useNavigate(); 
  const [refresh, setRefresh] = useState(false);
  const suppliersListRef = useRef(null);

  const handleLogout = () => {
    localStorage.removeItem("userType");
    navigate("/");
  };

  const scrollToSuppliers = () => {
    suppliersListRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  return (
    <div className={styles.container}>
      <h1>איזור ניהול אישי</h1>
      <button onClick={handleLogout} className={styles.logoutButton}>
        התנתקות
      </button>
      <button onClick={scrollToSuppliers} className={styles.addOrderButton}>
        הוספת הזמנה חדשה
      </button>{" "}
      <OrderListForOwner refresh={refresh} />
      <div className={styles.container} ref={suppliersListRef}>
        <SuppliersList setRefresh={setRefresh} />
      </div>
    </div>
  );
}

export default StoreOwnerPage;
