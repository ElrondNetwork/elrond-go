package txcache

import (
	"container/list"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListForSenderHints_AddTx(t *testing.T) {
	scenario := newHintsTestScenario(t)

	// First element in list, no hint saved
	scenario.add(7)
	scenario.noHint()
	scenario.noHintApplicableFor(3)
	scenario.noHintApplicableFor(5)

	// Add element to the left, hint ["prev of 7" (back.Next())] is saved
	scenario.add(3)
	scenario.hintForXIsElementY(7, math.MaxInt32)
	// Since the hint element is still nil,
	scenario.noHintApplicableFor(5)

	// Add element more to the left, hint ["prev of 3" (element "7")] is saved
	scenario.add(2)
	scenario.hintForXIsElementY(3, 7)
	// Hint is now usable for inserting element "2"
	scenario.hintApplicableFor(2)
	// Of course, hint is not usable for inserting element "4"
	scenario.noHintApplicableFor(4)
	// Now remove exactly the hint element
	scenario.lengthIs(3)
	scenario.remove(7)
	scenario.lengthIs(2)
	// Hints should be gone upon removal
	scenario.noHint()
	// Addition should work
	scenario.add(1)
	scenario.lengthIs(3)

	scenario.add(0)
	scenario.hintForXIsElementY(1, 2)
	// Elements "on" the hint or after the hint have no applicable hint
	scenario.noHintApplicableFor(1)
	scenario.noHintApplicableFor(2)
	scenario.hintApplicableFor(0)
}

var fooFallback = &list.Element{}

type hintsTestScenario struct {
	t    *testing.T
	list *txListForSender
}

func newHintsTestScenario(t *testing.T) *hintsTestScenario {
	return &hintsTestScenario{
		t:    t,
		list: newUnconstrainedListToTest(),
	}
}

func (scenario *hintsTestScenario) add(nonce int) {
	scenario.list.AddTx(createDummiestTx(uint64(nonce)))
}

func (scenario *hintsTestScenario) remove(nonce int) {
	scenario.list.RemoveTx(createDummiestTx(uint64(nonce)))
}

func (scenario *hintsTestScenario) noHint() {
	require.Equal(scenario.t, uint64(0), scenario.list.insertionHint.nonce)
	require.Nil(scenario.t, scenario.list.insertionHint.element)
}

func (scenario *hintsTestScenario) hintForXIsElementY(targetNonce int, elementNonce int) {
	require.Equal(scenario.t, uint64(targetNonce), scenario.list.insertionHint.nonce)
	require.Equal(scenario.t,
		elementToString(scenario.list.findListElementWithTx(createDummiestTx(uint64(elementNonce)))),
		elementToString(scenario.list.insertionHint.element),
	)
}

func (scenario *hintsTestScenario) noHintApplicableFor(nonce int) {
	require.Equal(scenario.t, fooFallback, scenario.list.insertionHint.recallReversedTraversal(uint64(nonce), fooFallback))
}

func (scenario *hintsTestScenario) hintApplicableFor(nonce int) {
	require.Equal(scenario.t, scenario.list.insertionHint.element, scenario.list.insertionHint.recallReversedTraversal(uint64(nonce), fooFallback))
}

func (scenario *hintsTestScenario) lengthIs(length int) {
	require.Equal(scenario.t, uint64(length), scenario.list.countTx())
}

func elementToString(element *list.Element) string {
	if element == nil {
		return "nil"
	}
	if element == fooFallback {
		return "foo"
	}

	return element.Value.(*WrappedTransaction).String()
}
