import React, { useState, useEffect } from "react";
import styles from "./EditProductsList.module.css";
import AddProductModal from "../AddProductModal/AddProductModal";
import axios from "axios";
import { useNavigate } from "react-router-dom";

function EditProductList() {
  const [isOpen, setIsOpen] = useState(false);
  const [products, setProducts] = useState([]);
  const [oldProducts, setOldProducts] = useState([]);
  const navigate = useNavigate();
  const supplierId = localStorage.getItem("userId");
  const supplierUsername = localStorage.getItem("username");

  useEffect(() => {
    axios
      .post("http://localhost:3000/products/get-products-by-supplier", {
        supplier_id: supplierId,
      })
      .then((response) => {
        setOldProducts(response.data);
      })
      .catch((error) => {
        console.error("Error fetching products:", error);
      });
  }, [products, supplierId]);

  const handleAddProduct = async (productData) => {
    try {
      const response = await axios.post("http://localhost:3000/products/add", {
        supplier_id: supplierId,
        ...productData,
      });

      setProducts([...products, productData]);
      setIsOpen(false);
    } catch (error) {
      console.error("Error adding product:", error);
    }
  };

  return (
    <div className={styles.container}>
      <div className={styles.backHomeWrapper}>
        {oldProducts.length > 0 && (
          <button
            className={styles.backHomeButton}
            onClick={() => navigate("/SupplierHome")}
          >
            לעמוד הבית
          </button>
        )}
      </div>
      <h2>{supplierUsername}: הוספת מוצרים עבור</h2>

      <button onClick={() => setIsOpen(true)} className={styles.addButton}>
        הוספת מוצר
      </button>

      {isOpen && (
        <AddProductModal
          onCancel={() => setIsOpen(false)}
          onAdd={handleAddProduct}
        />
      )}

      {products.length > 0 && (
        <>
          <h3>מוצרים להוספה למערכת </h3>
          <ul className={styles.productList}>
            {products.map((product) => (
              <li key={product.productId} className={styles.productItem}>
                <p>
                  מחיר: ₪{product.unit_price} | כמות מינימלית להזמנה:{" "}
                  {product.min_quantity}
                </p>
                <h4>{product.product_name}</h4>
              </li>
            ))}
          </ul>
          <div className={styles.finishButtonWrapper}>
            <button
              className={styles.finishButton}
              onClick={() => {
                setProducts([]);
                navigate("/EditProducts");
              }}
            >
              סיימתי להוסיף מוצרים
            </button>
          </div>
        </>
      )}
      <h3>מוצרים שקיימים במערכת</h3>
      <ul className={styles.productList}>
        {oldProducts.map((product) => (
          <li key={product.productId} className={styles.productItem}>
            <h4>{product.product_name}</h4>
            <p>
              מחיר: ₪{product.unit_price} | כמות מינימלית להזמנה:{" "}
              {product.min_quantity}
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default EditProductList;
