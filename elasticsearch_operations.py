from elasticsearch import Elasticsearch, NotFoundError

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200", http_auth=('elastic', '8BzQzbTcvlxMPGIydMeP'))

# Collections
v_nameCollection = 'hash_rowen'  # Replace with your name
v_phoneCollection = 'hash_3294'  # Replace 'XXXX' with your phone's last four digits

def create_collection(collection_name):
    """Create a collection (index) in Elasticsearch."""
    if not es.indices.exists(index=collection_name):
        es.indices.create(index=collection_name)
        print(f"Collection '{collection_name}' created.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def get_emp_count(collection_name):
    """Get the count of employees in the collection."""
    count = es.count(index=collection_name)['count']
    print(f"Employee count in '{collection_name}': {count}")
    return count

def del_emp_by_id(collection_name, emp_id):
    """Delete an employee by ID from the collection."""
    try:
        es.delete(index=collection_name, id=emp_id)
        print(f"Employee '{emp_id}' deleted from '{collection_name}'.")
    except NotFoundError:
        print(f"Employee '{emp_id}' not found in '{collection_name}'.")

def search_by_column(collection_name, column_name, value):
    """Search for records in the collection by a specified column."""
    query = {
        "query": {
            "match": {column_name: value}
        }
    }
    results = es.search(index=collection_name, body=query)
    print(f"Search results in '{collection_name}' for {column_name} = '{value}':")
    for hit in results['hits']['hits']:
        print(hit["_source"])

def get_all_docs(collection_name):
    """Get all documents in the collection."""
    results = es.search(index=collection_name, body={"query": {"match_all": {}}})
    print(f"All documents in '{collection_name}':")
    for hit in results['hits']['hits']:
        print(hit["_source"])

# Execution of functions in the specified order
create_collection(v_nameCollection)
create_collection(v_phoneCollection)
get_emp_count(v_nameCollection)
get_all_docs(v_nameCollection)  # Display all indexed documents
del_emp_by_id(v_nameCollection, 'E02007')  # Ensure this ID exists for testing
get_emp_count(v_nameCollection)
search_by_column(v_nameCollection, 'Department', 'IT')
search_by_column(v_nameCollection, 'Gender', 'Male')
search_by_column(v_phoneCollection, 'Department', 'IT')
