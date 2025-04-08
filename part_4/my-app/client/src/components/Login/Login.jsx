import React, { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import styles from "./Login.module.css";
import axios from "axios";

function Login() {
  console.log("login is loaded");

  localStorage.clear();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post("http://localhost:3000/user/login", {
        username,
        password,
      });

      const { userType, id } = response.data;
      localStorage.setItem("userType", userType);
      console.log(userType);
      localStorage.setItem("userId", id);
      localStorage.setItem("username", username);
      if (userType == "Supplier") {
        navigate("/SupplierHome");
      } else {
        navigate("/StoreOwnerHome");
      }
    } catch (error) {
      console.error("Error:", error);
      alert("שם משתמש או סיסמה שגויים");
    }
  };

  return (
    <div className={styles.container}>
      <form onSubmit={handleSubmit} className={styles.form}>
        <h2 className={styles.title}>התחברות</h2>
        <input
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          className={styles.input}
          placeholder="הכנס שם משתמש"
          required
        />
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          className={styles.input}
          placeholder="הכנס סיסמה"
          required
        />
        <button type="submit" className={styles.button}>
          התחבר
        </button>
        <Link to="/SupplierSignUp" className={styles.link}>
          רישום עבור ספק
        </Link>
      </form>
    </div>
  );
}

export default Login;
