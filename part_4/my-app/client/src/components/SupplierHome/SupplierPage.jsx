import React, { useEffect, useState } from "react";
import OrderListForSupplier from "../OrderListForSupplier/OrderListForSupplier";
import styles from "./SupplierHome.module.css";
import { useNavigate } from "react-router-dom"; 

function SupplierPage() {
  console.log("SupplierPage is loaded");

  const navigate = useNavigate(); 

  const supplierId = localStorage.getItem("userId");
  const supplierUsername = localStorage.getItem("username");

  const handleLogout = () => {
    localStorage.removeItem("userType");
    localStorage.removeItem("username");
    localStorage.removeItem("supplierId");
    navigate("/");
  };

  const handleEditProducts = () => {
    navigate("/EditProducts"); 
  };

  return (
    <div className={styles.container}>
      <h2 className={styles.title}>
        {supplierUsername}: ברוכים הבאים לאיזור האישי של
      </h2>
      <button onClick={handleLogout} className={styles.logoutButton}>
        התנתקות
      </button>

      <button
        onClick={handleEditProducts}
        className={styles.editProductsButton}
      >
        להוספת מוצרים זמינים
      </button>

      <OrderListForSupplier
        supplierId={supplierId}
        supplierUsername={supplierUsername}
      />
    </div>
  );
}

export default SupplierPage;
