import streamlit as st

st.title("My First Streamlit App 🚀")

st.write("Hello! This is a simple Streamlit app.")

name = st.text_input("What's your name?")

if name:
    st.success(f"Nice to meet you, {name}!")

age = st.slider("How old are you?", 0, 100, 25)
st.write(f"You are {age} years old.")

if st.button("Click me"):
    st.balloons()
