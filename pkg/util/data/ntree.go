package data

type NTree struct {
	value    interface{}
	children []*NTree
}

func (t *NTree) GetValue() interface{} {
	return t.value
}

func (t *NTree) Find(val interface{}) *NTree {
	if t.value == val {
		return t
	}
	for _, chld := range t.children {
		if res := chld.Find(val); res != nil {
			return res
		}
	}
	return nil
}

func (t *NTree) FindParent(val interface{}) *NTree {
	for _, child := range t.children {
		if child.value == val {
			return t
		}
	}
	for _, chld := range t.children {
		if res := chld.FindParent(val); res != nil {
			return res
		}
	}
	return nil
}

func (t *NTree) FindCommonParent(vals ...interface{}) *NTree {

	nodes := make([]*NTree, len(vals))

	for ix, val := range vals {
		nodes[ix] = t.Find(val)
		// If one of the nodes is the root, we return it right away
		if nodes[ix] == t {
			return t
		}
	}

	lca := nodes[0]
	for ix := 1; ix < len(nodes); ix++ {
		lca = t.findLCA(lca, nodes[ix])
		if lca == t {
			return t
		}
	}

	return lca
}

func (t *NTree) findLCA(t1 *NTree, t2 *NTree) *NTree {
	if t == t1 || t == t2 {
		return t
	}
	finds := make([]*NTree, 0)
	for _, chld := range t.children {
		if res := chld.findLCA(t1, t2); res != nil {
			finds = append(finds, res)
		}
	}
	if len(finds) == 0 {
		return nil
	} else if len(finds) == 1 {
		return finds[0]
	} else if len(finds) == 2 {
		return t
	} else {
		panic("Wrong tree structure: values are found in more than 2 children")
	}
}

func (t *NTree) FindOrInsert(val interface{}) *NTree {
	if ptr := t.Find(val); ptr == nil {
		ptr = &NTree{value: val}
		t.children = append(t.children, ptr)
		return ptr
	} else {
		return ptr
	}
}

func (t *NTree) Detach(val interface{}) *NTree {
	if ptr := t.FindParent(val); ptr != nil {
		for ix, chld := range ptr.children {
			if val == chld.value {
				res := ptr.children[ix]
				ptr.children = append(ptr.children[:ix], ptr.children[ix+1:]...)
				return res
			}
		}
	}
	return nil
}

func (t *NTree) PostTraversal() []interface{} {
	stack := make([]interface{}, 0)
	for _, chld := range t.children {
		stack = append(stack, chld.PostTraversal()...)
	}
	if t.value != nil {
		stack = append(stack, t.value)
	}
	return stack
}

func (t *NTree) PreTraversal() []interface{} {
	stack := make([]interface{}, 0)
	if t.value != nil {
		stack = append(stack, t.value)
	}
	for _, chld := range t.children {
		stack = append(stack, chld.PreTraversal()...)
	}
	return stack
}
