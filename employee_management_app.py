from flask import Flask, render_template, request, redirect, url_for
from elasticsearch import Elasticsearch, NotFoundError

app = Flask(__name__)

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200", http_auth=('elastic', '8BzQzbTcvlxMPGIydMeP'))

# Collections
v_nameCollection = 'hash_rowen'  # Replace with your name
v_phoneCollection = 'hash_3294'  # Replace 'XXXX' with your phone's last four digits

def create_collection(collection_name):
    if not es.indices.exists(index=collection_name):
        es.indices.create(index=collection_name)

def get_emp_count(collection_name):
    count = es.count(index=collection_name)['count']
    return count

def del_emp_by_id(collection_name, emp_id):
    try:
        es.delete(index=collection_name, id=emp_id)
    except NotFoundError:
        pass

def search_by_column(collection_name, column_name, value):
    query = {
        "query": {
            "match": {column_name: value}
        }
    }
    results = es.search(index=collection_name, body=query)
    return [hit["_source"] for hit in results['hits']['hits']]

def get_all_docs(collection_name):
    results = es.search(index=collection_name, body={"query": {"match_all": {}}})
    return [hit["_source"] for hit in results['hits']['hits']]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/employee_count')
def employee_count():
    count = get_emp_count(v_nameCollection)
    return render_template('employee_count.html', count=count)

@app.route('/all_docs')
def all_docs():
    docs = get_all_docs(v_nameCollection)
    return render_template('all_docs.html', docs=docs)

@app.route('/delete_employee', methods=['POST'])
def delete_employee():
    emp_id = request.form['emp_id']
    del_emp_by_id(v_nameCollection, emp_id)
    return redirect(url_for('index'))

@app.route('/search', methods=['POST'])
def search():
    column_name = request.form['column_name']
    value = request.form['value']
    results = search_by_column(v_nameCollection, column_name, value)
    return render_template('search_results.html', results=results)

if __name__ == '__main__':
    create_collection(v_nameCollection)
    create_collection(v_phoneCollection)
    app.run(debug=True)
