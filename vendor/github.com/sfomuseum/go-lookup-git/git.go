package git

import (
	"bytes"
	"context"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup/iterator"
	"io/ioutil"
	"log"
	"net/url"
)

const DEFAULT_BRANCH string = "main"

type GitIterator struct {
	iterator.Iterator
	uri string
	ref plumbing.ReferenceName
}

func init() {

	ctx := context.Background()

	schemes := []string{
		"git",
		"https",
	}

	for _, s := range schemes {

		err := iterator.RegisterIterator(ctx, s, NewGitIterator)

		if err != nil {
			panic(err)
		}
	}
}

func NewGitIterator(ctx context.Context, uri string) (iterator.Iterator, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	ref := plumbing.NewBranchReferenceName(DEFAULT_BRANCH)

	q := u.Query()

	branch := q.Get("branch")

	if branch != "" {
		ref = plumbing.NewBranchReferenceName(branch)
	}

	l := &GitIterator{
		uri: uri,
		ref: ref,
	}

	return l, nil
}

func (l *GitIterator) Append(ctx context.Context, lu catalog.Catalog, append_funcs ...iterator.AppendLookupFunc) error {

	r, err := gogit.Clone(memory.NewStorage(), nil, &gogit.CloneOptions{
		URL:   l.uri,
		Depth: 1,
	})

	if err != nil {
		return err
	}

	ref, err := r.Reference(l.ref, true)

	if err != nil {
		return err
	}

	commit, err := r.CommitObject(ref.Hash())

	if err != nil {
		return err
	}

	files, err := commit.Files()

	if err != nil {
		return err
	}

	err = files.ForEach(func(f *object.File) error {

		fh, err := f.Reader()

		if err != nil {
			return err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		for _, append_f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := append_f(ctx, lu, fh)

			if err != nil {
				log.Printf("GIT %s: %s\n", f.Name, err)
				// return err
			}
		}

		return nil
	})

	return err
}
