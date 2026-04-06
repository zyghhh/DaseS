import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { promptApi } from '@/lib/api-client'

export const promptKeys = {
  templates: ['prompts', 'templates'] as const,
  versions: (id: string) => ['prompts', id, 'versions'] as const,
}

export function usePromptTemplates() {
  return useQuery({
    queryKey: promptKeys.templates,
    queryFn: () => promptApi.listTemplates(),
  })
}

export function usePromptVersions(templateId: string | null) {
  return useQuery({
    queryKey: promptKeys.versions(templateId ?? ''),
    queryFn: () => promptApi.listVersions(templateId!),
    enabled: !!templateId,
  })
}

export function useCreatePrompt() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: promptApi.create,
    onSuccess: () => qc.invalidateQueries({ queryKey: promptKeys.templates }),
  })
}

export function useActivateVersion() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, version }: { id: string; version: number }) =>
      promptApi.activate(id, version),
    onSuccess: () => qc.invalidateQueries({ queryKey: promptKeys.templates }),
  })
}
