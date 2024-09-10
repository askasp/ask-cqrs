/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { BankAccountCommand } from '../models/BankAccountCommand';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class BankStreamService {
    /**
     * @param requestBody
     * @param authorization Bearer token
     * @returns any Command dispatched successfully
     * @throws ApiError
     */
    public static dispatch(
        requestBody: BankAccountCommand,
        authorization?: string | null,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/bank-stream',
            headers: {
                'Authorization': authorization,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Unauthorized`,
                422: `Domain validation error`,
            },
        });
    }
}
