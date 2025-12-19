from fastapi import APIRouter
from api.duckdb import conn

route = APIRouter(prefix="/spells")

@route.get("/")
def get_spell_list():
    