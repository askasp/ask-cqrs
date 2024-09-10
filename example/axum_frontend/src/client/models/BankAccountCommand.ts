/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type BankAccountCommand = ({
    event: BankAccountCommand.event;
    user_id: string;
} | {
    account_id: string;
    amount: number;
    event: BankAccountCommand.event;
} | {
    account_id: string;
    event: BankAccountCommand.event;
    user_id: string;
} | {
    account_id: string;
    event: BankAccountCommand.event;
    withdrawal_id: string;
});
export namespace BankAccountCommand {
    export enum event {
        OPEN_ACCOUNT = 'OpenAccount',
    }
}

