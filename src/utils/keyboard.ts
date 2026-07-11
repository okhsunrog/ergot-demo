/** Whether a keyboard event originated in a control where text can be edited. */
export function isTextEditingTarget(target: EventTarget | null): boolean {
  if (!target) return false
  const element = target as HTMLElement
  return (
    element.tagName === 'INPUT' ||
    element.tagName === 'TEXTAREA' ||
    element.tagName === 'SELECT' ||
    element.isContentEditable === true
  )
}
