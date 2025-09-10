from fastapi import APIRouter, Depends, HTTPException, status
from passlib.context import CryptContext
from jose import jwt
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from db import get_db_connection
import os as process
from dotenv import load_dotenv
from utils.auth import create_access_token

from models.users import Token, UserCreate, UserOut

load_dotenv()

SECRET_KEY = process.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY environment variable is not set. Please check your .env file.")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
router = APIRouter()

def hash_password(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


@router.post("/signup", response_model=UserOut)
def signup(user: UserCreate):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email = %s", (user.email,))
    existing_user = cur.fetchone()
    if existing_user:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed_pw = hash_password(user.password)
    cur.execute(
        "INSERT INTO users ( email, hashed_password) VALUES (%s, %s) RETURNING id, email",
        ( user.email, hashed_pw),
    )
    new_user = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return new_user

@router.post("/login", response_model=Token)
def login(user: UserCreate):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE email = %s", (user.email,))
    db_user = cur.fetchone()
    cur.close()
    conn.close()

    if not db_user or not verify_password(user.password, db_user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"sub": db_user["email"]})
    return {"access_token": token, "token_type": "bearer"}
