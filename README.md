# 🕵️‍♀️ Real-Time Yelp Review Stream Analyzer

A real-time data pipeline built with **Kafka**, **Spark Streaming**, **MongoDB**, and **Streamlit** to process, analyze, and visualize Yelp-style customer reviews.

---

## 📊 Project Overview

This project simulates the processing of live Yelp review data. It:
- Ingests reviews via **Kafka Producer**
- Performs **real-time sentiment analysis** using **NLP (VADER)**
- Detects potentially **fake reviews** using logistic regression
- Stores enriched reviews in **MongoDB**
- Visualizes sentiment trends live in a **Streamlit dashboard**

---

## 🚀 Tech Stack

| Component        | Technology |
|------------------|------------|
| Message Broker   | Apache Kafka |
| Stream Processing | Apache Spark Streaming |
| Database         | MongoDB |
| Frontend Dashboard | Streamlit |
| Language         | Python |
| NLP              | NLTK VADER Sentiment |
| ML Model         | Logistic Regression (Scikit-learn) |

---

[Click here to view the full PDF report](Dashboard.pdf)

