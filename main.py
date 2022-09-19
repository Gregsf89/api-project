from flask import Flask, Response, request, flash, redirect, url_for, session, jsonify, make_response
from flask_session import Session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
import pandas as pd
import os
import hashlib
import collections.abc


path = os.getcwd()
sess = Session()
UPLOAD_FOLDER = os.path.join(path, 'csv')
ALLOWED_EXTENSIONS = {'csv'}

if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:123456@localhost:3306/cognitivo'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

db = SQLAlchemy(app)


class Csv(db.Model):
    __tablename__ = "csv"
    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String(255))
    hash = db.Column(db.String(255), unique=True)
    created_at = db.Column(db.TIMESTAMP, default=db.func.current_timestamp())

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self

    def __init__(self, hash, file_name):
        self.hash = hash
        self.file_name = file_name
        self.created_at = db.func.current_timestamp()

    def __repr__(self):
        return '' % self.id


class AppstoreData(db.Model):
    __tablename__ = "appstore_data"
    id = db.Column(db.Integer, primary_key=True)
    track_name = db.Column(db.String(255))
    size_bytes = db.Column(db.Integer)
    price = db.Column(db.Float)
    n_citacoes = db.Column(db.Integer)
    prime_genre = db.Column(db.String(255))

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self

    def __init__(self, track_name, size_bytes, price, n_citacoes, prime_genre):
        self.track_name = track_name
        self.size_bytes = size_bytes
        self.price = price
        self.n_citacoes = n_citacoes
        self.prime_genre = prime_genre

    @property
    def serialize(self):
        # Returns Data Object In Proper Format
        return {
            'id': self.id,
            'track_name': self.track_name,
            'size_bytes': self.size_bytes,
            'price': self.price,
            'n_citacoes': self.n_citacoes,
            'prime_genre': self.prime_genre
        }

    def __repr__(self):
        return '' % self.id


db.create_all()
# ========================================= end of models =========================================


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def md5(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return make_response(jsonify({'msg': 'Not Acceptable'}), 406)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file')
            return make_response(jsonify({'msg': 'Not Acceptable'}), 406)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            hash = md5(app.config['UPLOAD_FOLDER'] + "/" + filename)
            if Csv.query.filter_by(hash=hash).first():
                return make_response(jsonify({'msg': 'File already exists with the given hash'}), 409)
            csv = Csv(hash, filename).create()

            data = pd.read_csv(
                app.config['UPLOAD_FOLDER'] + "/" + filename, index_col=0)
            data = data.drop(columns=["currency", "rating_count_ver", "user_rating", "user_rating_ver",
                             "ver", "cont_rating", "sup_devices.num", "ipadSc_urls.num", "lang.num", "vpp_lic"])
            data = data.rename(columns={"rating_count_tot": "n_citacoes"})
            data.to_sql('appstore_data', con=db.engine,
                        if_exists='append', index=False)
            return make_response(jsonify({'msg': f"file '{file.filename}' was uploaded with success", "hash": f"{hash}"}), 202)
    return make_response(jsonify({'msg': 'Bad Request'}), 400)


@app.route('/top', methods=['POST'])
def top():
    if request.method == 'POST':
        if not request.json:
            return make_response(jsonify({'msg': 'Not Acceptable'}), 406)
        if 'genre' not in request.json or not isinstance(request.json['genre'], collections.abc.Sequence):
            return make_response(jsonify({'msg': "key 'genre' is required"}), 406)
        if not request.json['genre']:
            return make_response(jsonify({'msg': "key 'genre' can't be an empty array"}), 406)
        length = len(request.json['genre'])
        for i in range(length):
            if not isinstance(request.json['genre'][i], str):
                return make_response(jsonify({'msg': "key 'genre' must be an array of strings"}), 406)
        if 'items' not in request.json or not isinstance(request.json['items'], int):
            return make_response(jsonify({'msg': "key 'items' is required"}), 406)
        if request.json['items'] <= 0:
            return make_response(jsonify({'msg': "key 'items' must be a positive integer"}), 406)
        data = AppstoreData.query.filter(AppstoreData.prime_genre.in_(request.json['genre'])).order_by(
            AppstoreData.n_citacoes.desc()).limit(request.json['items']).all()
        return make_response(jsonify({'data': [i.serialize for i in data]}), 200)
    return make_response(jsonify({'msg': 'Bad Request'}), 400)


if __name__ == '__main__':
    app.secret_key = '123456'
    app.config['SESSION_TYPE'] = 'filesystem'
    sess.init_app(app)
    app.debug = True
    app.run()
