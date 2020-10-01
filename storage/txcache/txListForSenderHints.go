package txcache

import (
	"container/list"
)

type listForSenderHint struct {
	element *list.Element
	nonce   uint64
}

// digestElement digests list traversal events.
// We are interested only in nonce-changing hints, so that they do not become misleading in case of [same-nonce, different-price] transactions.
func (hint *listForSenderHint) digestElement(element *list.Element, nonce uint64) {
	if nonce != hint.nonce {
		hint.element = element.Next()
		hint.nonce = nonce
	}
}

func (hint *listForSenderHint) notifyRemoval(removedElement *list.Element) {
	if hint.element == removedElement {
		hint.element = nil
		hint.nonce = 0
	}
}

// recallReversedTraversal recalls the position with respect to the target nonce, in back-to-front traversal
func (hint *listForSenderHint) recallReversedTraversal(targetNonce uint64, fallback *list.Element) *list.Element {
	if targetNonce >= hint.nonce {
		return fallback
	}
	if hint.element == nil {
		return fallback
	}

	return hint.element
}
