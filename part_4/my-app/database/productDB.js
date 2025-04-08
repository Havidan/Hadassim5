import pool from "./dbConnection.js";

export async function addProduct(
  supplier_id,
  product_name,
  unit_price,
  min_quantity,
) {
  try {
    const query = `
      INSERT INTO products (supplier_id, product_name, unit_price, min_quantity)
      VALUES (?, ?, ?, ?)
    `;
    console.log(supplier_id);

    const [result] = await pool.query(query, [
      supplier_id,
      product_name,
      unit_price,
      min_quantity,
    ]);
    console.log(result);

    return result.insertId;
  } catch (err) {
    throw new Error("Error adding product: " + err.message);
  }
}

export async function getProductsBySupplier(supplierId) {
  try {
    const query = `
        SELECT id, product_name, unit_price, min_quantity
        FROM products
        WHERE supplier_id = ?
      `;
    const [results] = await pool.query(query, [supplierId]);
    return results;
  } catch (err) {
    throw new Error("Error fetching products: " + err.message);
  }
}
