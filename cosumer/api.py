from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from typing import Optional, List, Dict
import subprocess
import json

app = FastAPI(
    title="Genomic Data API",
    description="API para consultar datos genómicos de MongoDB y HDFS",
    version="1.0.0"
)

# Configuración MongoDB
MONGO_URI = "mongodb://admin:genomic2025@mongodb:27017/genomic_db?authSource=admin"
HDFS_NAMENODE = "hdfs://namenode:9000"

def get_mongo_client():
    """Obtener cliente MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando a MongoDB: {str(e)}")

def get_hdfs_files(path: str):
    """Listar archivos en HDFS"""
    try:
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', path],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')[1:]  # Skip header
            files = []
            for line in lines:
                parts = line.split()
                if len(parts) >= 8:
                    files.append({
                        'permissions': parts[0],
                        'size': parts[4],
                        'date': parts[5],
                        'time': parts[6],
                        'path': parts[7]
                    })
            return files
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accediendo a HDFS: {str(e)}")

@app.get("/")
def root():
    """Endpoint raíz"""
    return {
        "message": "Genomic Data API",
        "version": "1.0.0",
        "endpoints": {
            "stats": "/stats",
            "fathers": "/fathers",
            "mothers": "/mothers",
            "children": "/children",
            "family": "/family/{family_id}",
            "hdfs": "/hdfs/{member_type}"
        }
    }

@app.get("/health")
def health_check():
    """Verificar salud de la API"""
    try:
        client = get_mongo_client()
        db = client['genomic_db']
        db.command('ping')
        return {"status": "healthy", "mongodb": "connected"}
    except:
        return {"status": "unhealthy", "mongodb": "disconnected"}

@app.get("/stats")
def get_stats():
    """Obtener estadísticas generales"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    fathers_count = db.fathers.count_documents({})
    mothers_count = db.mothers.count_documents({})
    children_count = db.children.count_documents({})
    
    return {
        "fathers": fathers_count,
        "mothers": mothers_count,
        "children": children_count,
        "total": fathers_count + mothers_count + children_count
    }

@app.get("/fathers")
def get_fathers(
    limit: int = Query(default=10, ge=1, le=100),
    skip: int = Query(default=0, ge=0)
):
    """Obtener registros de fathers"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    fathers = list(db.fathers.find({}, {'_id': 0}).skip(skip).limit(limit))
    total = db.fathers.count_documents({})
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": fathers
    }

@app.get("/mothers")
def get_mothers(
    limit: int = Query(default=10, ge=1, le=100),
    skip: int = Query(default=0, ge=0)
):
    """Obtener registros de mothers"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    mothers = list(db.mothers.find({}, {'_id': 0}).skip(skip).limit(limit))
    total = db.mothers.count_documents({})
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": mothers
    }

@app.get("/children")
def get_children(
    limit: int = Query(default=10, ge=1, le=100),
    skip: int = Query(default=0, ge=0)
):
    """Obtener registros de children"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    children = list(db.children.find({}, {'_id': 0}).skip(skip).limit(limit))
    total = db.children.count_documents({})
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": children
    }

@app.get("/family/{family_id}")
def get_family(family_id: str):
    """Obtener todos los miembros de una familia"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    father = db.fathers.find_one({"family_id": family_id}, {'_id': 0})
    mother = db.mothers.find_one({"family_id": family_id}, {'_id': 0})
    children = list(db.children.find({"family_id": family_id}, {'_id': 0}))
    
    if not father and not mother and not children:
        raise HTTPException(status_code=404, detail=f"Familia {family_id} no encontrada")
    
    return {
        "family_id": family_id,
        "father": father,
        "mother": mother,
        "children": children
    }

@app.get("/hdfs/{member_type}")
def get_hdfs_info(member_type: str):
    """Obtener información de archivos en HDFS"""
    if member_type not in ['fathers', 'mothers', 'children']:
        raise HTTPException(status_code=400, detail="member_type debe ser: fathers, mothers o children")
    
    path = f"/genomic_data/{member_type}"
    files = get_hdfs_files(path)
    
    total_size = sum(int(f['size']) for f in files)
    
    return {
        "member_type": member_type,
        "path": path,
        "file_count": len(files),
        "total_size_bytes": total_size,
        "files": files
    }

@app.get("/person/{person_id}")
def get_person(person_id: str):
    """Buscar una persona específica por person_id"""
    client = get_mongo_client()
    db = client['genomic_db']
    
    # Buscar en todas las colecciones
    father = db.fathers.find_one({"person_id": person_id}, {'_id': 0})
    if father:
        return {"member_type": "father", "data": father}
    
    mother = db.mothers.find_one({"person_id": person_id}, {'_id': 0})
    if mother:
        return {"member_type": "mother", "data": mother}
    
    child = db.children.find_one({"person_id": person_id}, {'_id': 0})
    if child:
        return {"member_type": "child", "data": child}
    
    raise HTTPException(status_code=404, detail=f"Persona {person_id} no encontrada")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
