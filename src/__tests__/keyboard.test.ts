import { expect, test } from 'vite-plus/test'
import { isTextEditingTarget } from '../utils/keyboard'

function target(tagName: string, isContentEditable = false): EventTarget {
  return { tagName, isContentEditable } as unknown as EventTarget
}

test('text-editing controls keep Delete and Backspace for themselves', () => {
  expect(isTextEditingTarget(target('INPUT'))).toBe(true)
  expect(isTextEditingTarget(target('TEXTAREA'))).toBe(true)
  expect(isTextEditingTarget(target('SELECT'))).toBe(true)
  expect(isTextEditingTarget(target('DIV', true))).toBe(true)
})

test('non-editable topology targets allow deletion shortcuts', () => {
  expect(isTextEditingTarget(target('DIV'))).toBe(false)
  expect(isTextEditingTarget(null)).toBe(false)
})
