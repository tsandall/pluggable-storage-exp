package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/open-policy-agent/opa/repl"
	"github.com/open-policy-agent/opa/server"
	"github.com/open-policy-agent/opa/storage"
)

var errBadSchema = fmt.Errorf("bad schema")

type Kubernetes struct {
	mountPath storage.Path
	supported []string
}

func NewKubernetes() *Kubernetes {
	return &Kubernetes{
		mountPath: storage.Path{"k8s"},
	}
}

func (b *Kubernetes) Close(txn storage.Transaction) {
}

func (b *Kubernetes) Get(txn storage.Transaction, p storage.Path, recursive bool) (interface{}, error) {

	basepath := p[len(b.mountPath):]

	if len(basepath) == 0 {
		return nil, nil
	}

	resource := basepath[0]

	switch resource {
	case "pods":
		return b.getNamespacedColl("pods", txn, basepath[1:], recursive)
	case "replicationcontrollers":
		return b.getNamespacedColl("replicationcontrollers", txn, basepath[1:], recursive)
	case "serviceaccounts":
		return b.getNamespacedColl("serviceaccounts", txn, basepath[1:], recursive)
	case "secrets":
		return b.getNamespacedColl("secrets", txn, basepath[1:], recursive)
	case "namespaces":
		return b.getColl("namespaces", true, txn, basepath[1:], recursive)
	case "nodes":
		return b.getColl("nodes", true, txn, basepath[1:], recursive)
	}

	return nil, &storage.Error{
		Code:    storage.NotFoundErr,
		Message: "not found",
	}
}

func (b *Kubernetes) getNamespacedColl(coll string, txn storage.Transaction, basepath storage.Path, recursive bool) (interface{}, error) {

	if len(basepath) == 0 {
		// list all
		resp, err := b.get("/" + coll)
		if err != nil {
			return nil, err
		}
		items := resp.(map[string]interface{})["items"].([]interface{})
		return b.formatNamespacedItems(items), nil
	}

	if len(basepath) == 1 {
		// list namespace
		resp, err := b.get("/namespaces/" + basepath[0].(string) + "/" + coll)
		if err != nil {
			return nil, err
		}
		items := resp.(map[string]interface{})["items"].([]interface{})
		return b.formatItems(items, true), nil
	}

	// get specific
	resp, err := b.get("/namespaces/" + basepath[0].(string) + "/" + coll + "/" + basepath[1].(string))
	if err != nil {
		return nil, err
	}

	// dereference
	obj := resp.(map[string]interface{})
	r, ok := basepath[2:].Eval(obj)
	if !ok {
		return nil, &storage.Error{
			Code:    storage.NotFoundErr,
			Message: "not found (eval)",
		}
	}
	return r, nil
}

func (b *Kubernetes) getColl(coll string, uniqueName bool, txn storage.Transaction, p storage.Path, recursive bool) (interface{}, error) {
	if len(p) == 0 {
		resp, err := b.get("/" + coll)
		if err != nil {
			return nil, err
		}
		items := resp.(map[string]interface{})["items"].([]interface{})
		return b.formatItems(items, uniqueName), nil
	}
	resp, err := b.get(fmt.Sprintf("/%v/%v", coll, p[0]))
	if err != nil {
		return nil, err
	}
	obj := resp.(map[string]interface{})
	r, ok := p[1:].Eval(obj)
	if !ok {
		return nil, &storage.Error{
			Code:    storage.NotFoundErr,
			Message: "not found (eval)",
		}
	}
	return r, nil
}

func (b *Kubernetes) formatItems(items []interface{}, uniqueName bool) map[string]interface{} {
	r := map[string]interface{}{}
	for _, x := range items {
		obj := x.(map[string]interface{})
		metadata := obj["metadata"].(map[string]interface{})
		var id string
		if uniqueName {
			id = metadata["name"].(string)
		} else {
			id = metadata["uid"].(string)
		}
		r[id] = obj
	}
	return r
}

func (b *Kubernetes) formatNamespacedItems(items []interface{}) map[string]interface{} {
	r := map[string]interface{}{}
	for _, x := range items {
		obj := x.(map[string]interface{})
		metadata := obj["metadata"].(map[string]interface{})
		name := metadata["name"].(string)
		ns := metadata["namespace"].(string)
		if v, ok := r[ns]; !ok {
			r[ns] = map[string]interface{}{name: obj}
		} else {
			coll := v.(map[string]interface{})
			coll[name] = obj
		}
	}
	return r
}

func (b *Kubernetes) get(path string) (interface{}, error) {

	url := fmt.Sprintf("http://192.168.99.150:8080/api/v1%v", path)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return nil, &storage.Error{
				Code:    storage.NotFoundErr,
				Message: "404",
			}
		}
		return nil, fmt.Errorf("bad response: %v", resp.StatusCode)
	}

	var v interface{}
	d := json.NewDecoder(resp.Body)
	if err := d.Decode(&v); err != nil {
		return nil, err
	}

	return v, nil
}

func main() {

	ds := storage.NewDataStore()
	ps := storage.NewPolicyStore(ds, "")
	shim := storage.NewDatastoreShim(ds)
	store := storage.New(shim)
	k8s := NewKubernetes()
	err := store.Mount(k8s, storage.Path{"k8s"})

	if err != nil {
		fmt.Println("Error mounting:", err)
		os.Exit(1)
	}

	s := server.New(store, ds, ps, ":8181", false)
	go s.Loop()

	r := repl.New(store, ds, ps, "", os.Stdout, "json")
	r.OneShot("import data.k8s")
	r.Loop()

}
