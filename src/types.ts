export interface IEventMessage {
  key: string;
  value: string;
  partition?: number;
  headers?: Record<string, string>;
}

export interface ITopicMessages {
  topic: string;
  messages: IEventMessage[];
}