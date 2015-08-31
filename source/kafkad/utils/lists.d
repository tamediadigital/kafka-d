module kafkad.utils.lists;

struct List(Node) 
    if (is(Node == struct) && is(typeof(Node.next) == Node*))
{
    private Node* m_front = null, m_back = null;
    @property bool empty() { return m_front == null; }
    Node* popFront() {
        assert(m_front);
        auto node = m_front;
        m_front = node.next;
        return node;
    }
    void pushBack(Node* node) {
        if (m_front) {
            assert(m_back);
            m_back.next = node;
            m_back = node;
        } else {
            m_front = node;
            m_back = node;
        }
        node.next = null;
    }
}

struct FreeList(Node) 
    if (is(Node == struct) && is(typeof(Node.next) == Node*))
{
    private List!Node m_filled, m_free;
    @property bool empty() { return m_filled.empty; }
    Node* getNodeToFill() {
        return m_free.empty ? new Node : m_free.popFront();
    }
    void pushFilledNode(Node* node) {
        m_filled.pushBack(node);
    }
    Node* getNodeToProcess() {
        return m_filled.popFront();
    }
    void returnProcessedNode(Node* node) {
        m_free.pushBack(node);
    }
}
